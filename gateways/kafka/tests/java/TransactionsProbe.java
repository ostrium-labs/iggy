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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * KW-T: the Java producer's transactions against a broker. Commits, an abort, and a second
 * producer with the same transactional id fencing the first; then what a read_uncommitted and a
 * read_committed consumer see. Prints one line per fact, and TXN_PROBE_OK at the end.
 *
 * <p>Args: bootstrap, topic, transactional id.
 */
public final class TransactionsProbe {
    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = args[1];
        String transactionalId = args[2];

        Properties admin = new Properties();
        admin.put("bootstrap.servers", bootstrap);
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }

        KafkaProducer<String, String> first = producer(bootstrap, transactionalId);
        first.initTransactions();
        first.beginTransaction();
        first.send(new ProducerRecord<>(topic, 0, "k", "c1")).get();
        first.send(new ProducerRecord<>(topic, 1, "k", "c2")).get();
        first.send(new ProducerRecord<>(topic, 0, "k", "c3")).get();
        first.commitTransaction();
        first.beginTransaction();
        first.send(new ProducerRecord<>(topic, 0, "k", "a1")).get();
        first.send(new ProducerRecord<>(topic, 1, "k", "a2")).get();
        first.abortTransaction();
        first.beginTransaction();
        first.send(new ProducerRecord<>(topic, 1, "k", "c4")).get();
        first.commitTransaction();

        KafkaProducer<String, String> second = producer(bootstrap, transactionalId);
        second.initTransactions();
        String fenced = "none";
        try {
            first.beginTransaction();
            first.send(new ProducerRecord<>(topic, 0, "k", "z1")).get();
            first.commitTransaction();
        } catch (Exception e) {
            Throwable cause = e;
            while (cause.getCause() != null && !(cause instanceof org.apache.kafka.common.errors.ProducerFencedException)
                    && !(cause instanceof org.apache.kafka.common.errors.InvalidProducerEpochException)) {
                cause = cause.getCause();
            }
            fenced = cause.getClass().getSimpleName();
        }
        System.out.println("fenced=" + fenced);
        first.close(Duration.ofSeconds(5));
        second.beginTransaction();
        second.send(new ProducerRecord<>(topic, 0, "k", "c5")).get();
        second.commitTransaction();
        second.close(Duration.ofSeconds(5));

        System.out.println("read_uncommitted=" + read(bootstrap, topic, "read_uncommitted"));
        System.out.println("read_committed=" + read(bootstrap, topic, "read_committed"));
        System.out.println("TXN_PROBE_OK");
    }

    private static KafkaProducer<String, String> producer(String bootstrap, String transactionalId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("transactional.id", transactionalId);
        props.put("transaction.timeout.ms", "60000");
        props.put("max.block.ms", "30000");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    /** Every value of both partitions at the isolation level, sorted. */
    private static List<String> read(String bootstrap, String topic, String isolation) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("isolation.level", isolation);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions =
                    List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));
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
        return values;
    }
}
