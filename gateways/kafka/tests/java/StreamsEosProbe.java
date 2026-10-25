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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

/**
 * KW-T: a Kafka Streams word count at processing.guarantee=exactly_once_v2, with an in-memory
 * store (no RocksDB) so the changelog, the repartition topic and the transactional commits are
 * what is exercised. The Streams instance is closed and a new one started half way, as after a
 * restart. Prints the final count per word read back at read_committed, and STREAMS_PROBE_OK.
 *
 * <p>Args: bootstrap, application id, input topic, output topic.
 */
public final class StreamsEosProbe {
    private static final String[] LINES = {
        "the quick brown fox", "jumps over the lazy dog", "the dog barks", "a fox runs",
        "quick quick slow", "the end",
    };

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String application = args[1];
        String input = args[2];
        String output = args[3];

        Properties admin = new Properties();
        admin.put("bootstrap.servers", bootstrap);
        try (Admin client = Admin.create(admin)) {
            client.createTopics(List.of(new NewTopic(input, 2, (short) 1), new NewTopic(output, 2, (short) 1)))
                    .all()
                    .get();
        }

        produce(bootstrap, input, 0, 3);
        KafkaStreams first = start(bootstrap, application, input, output);
        Thread.sleep(8000);
        first.close(Duration.ofSeconds(30));
        produce(bootstrap, input, 3, LINES.length);
        KafkaStreams second = start(bootstrap, application, input, output);
        Map<String, Long> expected = new TreeMap<>();
        for (String line : LINES) {
            for (String word : line.split(" ")) {
                expected.merge(word, 1L, Long::sum);
            }
        }
        Map<String, Long> counts = read(bootstrap, output, expected, 90000);
        second.close(Duration.ofSeconds(30));
        System.out.println("counts=" + counts);
        System.out.println("expected=" + expected);
        System.out.println("exactly_once=" + counts.equals(expected));
        System.out.println("STREAMS_PROBE_OK");
    }

    private static void produce(String bootstrap, String input, int from, int to) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = from; i < to; i++) {
                producer.send(new ProducerRecord<>(input, "l" + i, LINES[i])).get();
            }
        }
    }

    private static KafkaStreams start(String bootstrap, String application, String input, String output) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, application);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 200);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir") + "/streams-" + application);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("consumer.auto.offset.reset", "earliest");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lines = builder.stream(input);
        lines.flatMapValues(line -> Arrays.asList(line.split(" ")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("counts"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to(output, Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(exception -> {
            exception.printStackTrace();
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        return streams;
    }

    /**
     * The last count per word at read_committed: once it equals `expected` and nothing new arrives
     * for 3 s, or after `limitMs`.
     */
    private static Map<String, Long> read(String bootstrap, String output, Map<String, Long> expected, long limitMs) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("isolation.level", "read_committed");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", LongDeserializer.class.getName());
        Map<String, Long> counts = new TreeMap<>();
        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = List.of(new TopicPartition(output, 0), new TopicPartition(output, 1));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long started = System.currentTimeMillis();
            long quietSince = started;
            while (System.currentTimeMillis() - started < limitMs) {
                for (ConsumerRecord<String, Long> record : consumer.poll(Duration.ofMillis(200))) {
                    counts.put(record.key(), record.value());
                    quietSince = System.currentTimeMillis();
                }
                if (counts.equals(expected) && System.currentTimeMillis() - quietSince > 3000) {
                    break;
                }
            }
        }
        return counts;
    }
}
