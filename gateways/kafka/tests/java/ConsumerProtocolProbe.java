/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * KW-13: the Java 4.x consumer with {@code group.protocol=consumer} (KIP-848) against a broker.
 *
 * <p>Two members share a two-partition topic and then one leaves; a third resumes from the
 * committed offsets; a group that a classic consumer committed in is resumed by a KIP-848 one;
 * {@code describeConsumerGroups} reports the live group. Prints {@code key=value} lines that the
 * Rust side compares between the gateway and Apache Kafka, then {@code CONSUMER_PROBE_OK}.
 *
 * <p>Arguments: bootstrap address, topic, group prefix.
 */
public final class ConsumerProtocolProbe {
    private static final Duration POLL = Duration.ofMillis(200);
    private static final long DEADLINE_MS = 60_000;

    private ConsumerProtocolProbe() {}

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = args[1];
        String prefix = args[2];
        String group = prefix + "-848";
        String migrated = prefix + "-mig";

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get(60, TimeUnit.SECONDS);
            produce(bootstrap, topic, 0, 20);

            // Two members: every record is read once, and both end up with a partition.
            try (KafkaConsumer<String, String> a = consumer(bootstrap, group, "consumer", "a");
                    KafkaConsumer<String, String> b = consumer(bootstrap, group, "consumer", "b")) {
                a.subscribe(List.of(topic));
                Set<String> seen = new HashSet<>();
                long until = System.currentTimeMillis() + DEADLINE_MS;
                while (System.currentTimeMillis() < until && a.assignment().isEmpty()) {
                    drain(a, seen);
                }
                b.subscribe(List.of(topic));
                until = System.currentTimeMillis() + DEADLINE_MS;
                while (System.currentTimeMillis() < until
                        && (seen.size() < 20 || a.assignment().isEmpty() || b.assignment().isEmpty())) {
                    drain(a, seen);
                    drain(b, seen);
                }
                System.out.println("two_members_consumed=" + seen.size());
                System.out.println("two_members_a_partitions=" + a.assignment().size());
                System.out.println("two_members_b_partitions=" + b.assignment().size());
                Set<TopicPartition> both = new HashSet<>(a.assignment());
                both.retainAll(b.assignment());
                System.out.println("two_members_disjoint=" + both.isEmpty());
                a.commitSync();
                b.commitSync();

                // b leaves; a takes its partition.
                b.close();
                until = System.currentTimeMillis() + DEADLINE_MS;
                while (System.currentTimeMillis() < until && a.assignment().size() < 2) {
                    drain(a, seen);
                }
                System.out.println("after_leave_a_partitions=" + a.assignment().size());
                a.commitSync();
            }

            // A new member resumes where the group committed.
            produce(bootstrap, topic, 20, 6);
            try (KafkaConsumer<String, String> c = consumer(bootstrap, group, "consumer", "c")) {
                c.subscribe(List.of(topic));
                List<ConsumerRecord<String, String>> read = readAtLeast(c, 6);
                System.out.println("resumed=" + read.size());
                long first = read.stream().mapToLong(ConsumerRecord::offset).min().orElse(-1);
                System.out.println("resumed_first_offset=" + first);
                c.commitSync();

                ConsumerGroupDescription description =
                        admin.describeConsumerGroups(List.of(group)).all().get(30, TimeUnit.SECONDS).get(group);
                System.out.println("describe_type=" + description.type());
                System.out.println("describe_state=" + description.groupState());
                System.out.println("describe_members=" + description.members().size());
                int partitions = 0;
                for (MemberDescription member : description.members()) {
                    partitions += member.assignment().topicPartitions().size();
                    System.out.println("describe_client_id=" + member.clientId());
                }
                System.out.println("describe_partitions=" + partitions);
            }

            // A classic consumer commits; a KIP-848 consumer of the same group goes on from there.
            try (KafkaConsumer<String, String> d = consumer(bootstrap, migrated, "classic", "d")) {
                d.subscribe(List.of(topic));
                System.out.println("classic_read=" + readAtLeast(d, 26).size());
                d.commitSync();
            }
            produce(bootstrap, topic, 26, 4);
            try (KafkaConsumer<String, String> e = consumer(bootstrap, migrated, "consumer", "e")) {
                e.subscribe(List.of(topic));
                System.out.println("classic_then_consumer=" + readAtLeast(e, 4).size());
                e.commitSync();
            }
        }
        System.out.println("CONSUMER_PROBE_OK");
    }

    private static KafkaConsumer<String, String> consumer(
            String bootstrap, String group, String protocol, String name) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "probe-" + name);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static void produce(String bootstrap, String topic, int from, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = from; i < from + count; i++) {
                producer.send(new ProducerRecord<>(topic, i % 2, "k" + i, "v" + i)).get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static void drain(KafkaConsumer<String, String> consumer, Set<String> seen) {
        for (ConsumerRecord<String, String> record : consumer.poll(POLL)) {
            seen.add(record.partition() + "@" + record.offset());
        }
    }

    private static List<ConsumerRecord<String, String>> readAtLeast(
            KafkaConsumer<String, String> consumer, int count) {
        List<ConsumerRecord<String, String>> read = new ArrayList<>();
        long until = System.currentTimeMillis() + DEADLINE_MS;
        while (System.currentTimeMillis() < until && read.size() < count) {
            for (ConsumerRecord<String, String> record : consumer.poll(POLL)) {
                read.add(record);
            }
        }
        // Anything past `count` would be a duplicate: read a little longer to see it.
        long settle = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < settle) {
            for (ConsumerRecord<String, String> record : consumer.poll(POLL)) {
                read.add(record);
            }
        }
        return read;
    }
}
