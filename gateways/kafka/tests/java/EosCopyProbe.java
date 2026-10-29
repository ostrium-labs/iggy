// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * KW-T: consume-transform-produce with the Java clients, as Kafka Streams and Flink do it. A group
 * member reads an input topic at read_committed, and a transactional producer writes each record
 * transformed and commits the member's offsets in the same transaction
 * (sendOffsetsToTransaction with the group metadata, KIP-447). Every third transaction aborts and
 * the consumer rewinds to its committed offsets, as an application does after a failure. The
 * output, read at read_committed, must hold every input record exactly once.
 *
 * <p>Args: bootstrap, input topic, output topic, group, transactional id, record count.
 */
public final class EosCopyProbe {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String input = args[1];
        String output = args[2];
        String group = args[3];
        String transactionalId = args[4];
        int count = Integer.parseInt(args[5]);

        Properties admin = new Properties();
        admin.put("bootstrap.servers", bootstrap);
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(input, 2, (short) 1), new NewTopic(output, 2, (short) 1)))
                    .all()
                    .get();
        }
        Properties plain = new Properties();
        plain.put("bootstrap.servers", bootstrap);
        plain.put("key.serializer", StringSerializer.class.getName());
        plain.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(plain)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(input, "k" + i, "v" + i)).get();
            }
        }

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrap);
        consumerProps.put("group.id", group);
        consumerProps.put("isolation.level", "read_committed");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("max.poll.records", "7");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        Properties producerProps = new Properties();
        producerProps.putAll(plain);
        producerProps.put("transactional.id", transactionalId);

        int transactions = 0;
        int aborted = 0;
        int copied = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            producer.initTransactions();
            consumer.subscribe(List.of(input));
            long quietSince = System.currentTimeMillis();
            while (copied < count && System.currentTimeMillis() - quietSince < 30000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.isEmpty()) {
                    continue;
                }
                quietSince = System.currentTimeMillis();
                producer.beginTransaction();
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : records) {
                    producer.send(new ProducerRecord<>(output, record.key(), record.value().toUpperCase()));
                    offsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                transactions++;
                if (transactions % 3 == 0) {
                    producer.abortTransaction();
                    aborted++;
                    // Rewind to what the group committed, as after a failure.
                    for (TopicPartition partition : consumer.assignment()) {
                        OffsetAndMetadata committed = consumer.committed(java.util.Set.of(partition)).get(partition);
                        consumer.seek(partition, committed == null ? 0 : committed.offset());
                    }
                } else {
                    producer.commitTransaction();
                    copied += records.count();
                }
            }
        }
        System.out.println("transactions=" + transactions);
        System.out.println("aborted=" + aborted);

        Properties readProps = new Properties();
        readProps.put("bootstrap.servers", bootstrap);
        readProps.put("isolation.level", "read_committed");
        readProps.put("key.deserializer", StringDeserializer.class.getName());
        readProps.put("value.deserializer", StringDeserializer.class.getName());
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(readProps)) {
            List<TopicPartition> partitions = List.of(new TopicPartition(output, 0), new TopicPartition(output, 1));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long quietSince = System.currentTimeMillis();
            while (System.currentTimeMillis() - quietSince < 3000) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    values.add(record.value());
                    quietSince = System.currentTimeMillis();
                }
            }
        }
        Collections.sort(values);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            expected.add("V" + i);
        }
        Collections.sort(expected);
        System.out.println("output_records=" + values.size());
        System.out.println("exactly_once=" + values.equals(expected));
        System.out.println("EOS_PROBE_OK");
    }
}
