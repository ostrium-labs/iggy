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
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * KW-T fault matrix: the consume-transform-produce loop of EosCopyProbe, made to survive a broker
 * that goes away, as Kafka Streams survives one. On any error the loop drops its producer and
 * consumer, makes new ones (initTransactions fences the old incarnation and aborts its open
 * transaction), and resumes from the group's committed offsets. The test kills and restarts the
 * gateway while this runs. The output, read at read_committed, must hold every input record
 * exactly once.
 *
 * <p>Args: bootstrap, input topic, output topic, group, transactional id, record count, seconds.
 */
public final class EosChaosProbe {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String input = args[1];
        String output = args[2];
        String group = args[3];
        String transactionalId = args[4];
        int count = Integer.parseInt(args[5]);
        long deadline = System.currentTimeMillis() + Long.parseLong(args[6]) * 1000;

        Properties admin = new Properties();
        admin.put("bootstrap.servers", bootstrap);
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(input, 2, (short) 1), new NewTopic(output, 2, (short) 1)))
                    .all()
                    .get();
        } catch (java.util.concurrent.ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
        Properties plain = new Properties();
        plain.put("bootstrap.servers", bootstrap);
        plain.put("key.serializer", StringSerializer.class.getName());
        plain.put("value.serializer", StringSerializer.class.getName());
        plain.put("delivery.timeout.ms", "120000");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(plain)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(input, "k" + i, "v" + i));
            }
            producer.flush();
        }
        System.out.println("input_written=" + count);

        int restarts = 0;
        int commits = 0;
        long committedRecords = 0;
        while (System.currentTimeMillis() < deadline) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrap, group));
                    KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap, transactionalId))) {
                producer.initTransactions();
                consumer.subscribe(List.of(input));
                long idleSince = System.currentTimeMillis();
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                    if (records.isEmpty()) {
                        if (done(consumer, input, count) && System.currentTimeMillis() - idleSince > 2000) {
                            deadline = 0;
                        }
                        continue;
                    }
                    idleSince = System.currentTimeMillis();
                    producer.beginTransaction();
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for (ConsumerRecord<String, String> record : records) {
                        producer.send(new ProducerRecord<>(output, record.key(), record.value().toUpperCase()));
                        offsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    producer.commitTransaction();
                    commits++;
                    committedRecords += records.count();
                }
            } catch (Exception e) {
                restarts++;
                System.out.println("restart=" + e.getClass().getSimpleName() + ": " + e.getMessage());
                Thread.sleep(500);
            }
        }
        System.out.println("restarts=" + restarts);
        System.out.println("commits=" + commits);

        List<String> values = readCommitted(bootstrap, output);
        Collections.sort(values);
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            expected.add("V" + i);
        }
        Collections.sort(expected);
        System.out.println("output_records=" + values.size());
        System.out.println("distinct_output=" + new java.util.HashSet<>(values).size());
        System.out.println("exactly_once=" + values.equals(expected));
        System.out.println("CHAOS_PROBE_OK");
    }

    private static Properties consumerProps(String bootstrap, String group) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", group);
        props.put("isolation.level", "read_committed");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "10");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }

    private static Properties producerProps(String bootstrap, String transactionalId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("transactional.id", transactionalId);
        props.put("transaction.timeout.ms", "10000");
        props.put("max.block.ms", "20000");
        props.put("delivery.timeout.ms", "30000");
        props.put("request.timeout.ms", "10000");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }

    /** Whether the group committed every input record. */
    private static boolean done(KafkaConsumer<String, String> consumer, String input, int count) {
        Set<TopicPartition> partitions = Set.of(new TopicPartition(input, 0), new TopicPartition(input, 1));
        long committed = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumer.committed(partitions).entrySet()) {
            if (entry.getValue() != null) {
                committed += entry.getValue().offset();
            }
        }
        return committed >= count;
    }

    private static List<String> readCommitted(String bootstrap, String output) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("isolation.level", "read_committed");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = List.of(new TopicPartition(output, 0), new TopicPartition(output, 1));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long quietSince = System.currentTimeMillis();
            while (System.currentTimeMillis() - quietSince < 5000) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    values.add(record.value());
                    quietSince = System.currentTimeMillis();
                }
            }
        }
        return values;
    }
}
