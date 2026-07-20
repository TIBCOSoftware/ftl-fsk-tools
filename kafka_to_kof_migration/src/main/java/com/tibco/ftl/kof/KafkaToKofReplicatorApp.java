package com.tibco.ftl.kof;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Replicates all records from source Kafka topics/partitions to KOF.
 *
 * Workflow:
 * 1) Discover topics and partitions from source brokers
 * 2) Print pre-copy stats (message counts by topic/partition)
 * 3) Create matching topics on target KOF brokers
 * 4) Read all records from source (offset snapshot) and publish to target
 */
public class KafkaToKofReplicatorApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaToKofReplicatorApp.class);

    private final Config config;

    private KafkaToKofReplicatorApp(Config config) {
        this.config = config;
    }

    public static void main(String[] args) {
        try {
            Config config = Config.fromArgs(args);

            if (config.helpRequested) {
                Config.printUsage();
                return;
            }

            KafkaToKofReplicatorApp app = new KafkaToKofReplicatorApp(config);
            app.run();
        } catch (Exception e) {
            logger.error("Replication failed", e);
            System.exit(1);
        }
    }

    private static void out(String fmt, Object... args) {
        String msg = fmt;
        for (Object a : args) msg = msg.replaceFirst("\\{\\}", a == null ? "null" : a.toString());
        System.out.println(msg);
        System.out.flush();
    }

    private void run() throws Exception {
        out("Starting Kafka -> KOF replication");
        out("Source bootstrap servers: {}", config.sourceBootstrapServers);
        out("Target bootstrap servers: {}", config.targetBootstrapServers);
        out("Topic pattern: {}", config.topicPattern.pattern());
        out("Include internal topics: {}", config.includeInternalTopics);
        out("Dry run: {}", config.dryRun);

        try (AdminClient sourceAdmin = AdminClient.create(config.buildSourceAdminProperties());
             KafkaConsumer<byte[], byte[]> sourceConsumer = new KafkaConsumer<>(config.buildSourceConsumerProperties());
             AdminClient targetAdmin = AdminClient.create(config.buildTargetAdminProperties());
             KafkaProducer<byte[], byte[]> targetProducer = new KafkaProducer<>(config.buildTargetProducerProperties())) {

            Map<String, TopicDescription> sourceTopics = discoverSourceTopics(sourceAdmin);
            if (sourceTopics.isEmpty()) {
                out("WARN: No topics matched on source brokers. Nothing to replicate.");
                return;
            }

            List<TopicPartition> partitions = toTopicPartitions(sourceTopics);
            if (partitions.isEmpty()) {
                out("WARN: No partitions found in matched topics. Nothing to replicate.");
                return;
            }

            sourceConsumer.assign(partitions);
            sourceConsumer.seekToBeginning(partitions);

            Map<TopicPartition, Long> beginOffsets = sourceConsumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = sourceConsumer.endOffsets(partitions);
            Map<TopicPartition, Long> plannedCounts = computePartitionCounts(beginOffsets, endOffsets);

            printPreCopyStats(plannedCounts);

            if (config.dryRun) {
                out("Dry run requested. Skipping topic creation and record replication.");
                return;
            }

            createTopicsOnTarget(targetAdmin, sourceTopics);

            Map<TopicPartition, Long> actualReadCounts = replicateSnapshot(
                sourceConsumer,
                targetProducer,
                beginOffsets,
                endOffsets
            );

            printFinalStats(plannedCounts, actualReadCounts);
        }

        out("Replication completed.");
    }

    private Map<String, TopicDescription> discoverSourceTopics(AdminClient sourceAdmin) throws Exception {
        ListTopicsOptions options = new ListTopicsOptions().listInternal(config.includeInternalTopics);
        Set<String> topicNames = sourceAdmin.listTopics(options).names().get();

        List<String> filtered = topicNames.stream()
            .filter(name -> config.topicPattern.matcher(name).matches())
            .sorted()
            .collect(Collectors.toList());

        if (filtered.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, TopicDescription> described = sourceAdmin.describeTopics(filtered).allTopicNames().get();
        out("Discovered {} topic(s) on source", described.size());

        return described.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> a,
                LinkedHashMap::new
            ));
    }

    private List<TopicPartition> toTopicPartitions(Map<String, TopicDescription> topics) {
        List<TopicPartition> partitions = new ArrayList<>();

        for (Map.Entry<String, TopicDescription> entry : topics.entrySet()) {
            String topic = entry.getKey();
            TopicDescription description = entry.getValue();

            description.partitions()
                .forEach(info -> partitions.add(new TopicPartition(topic, info.partition())));
        }

        partitions.sort(
            Comparator.comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition)
        );

        return partitions;
    }

    private Map<TopicPartition, Long> computePartitionCounts(
        Map<TopicPartition, Long> beginOffsets,
        Map<TopicPartition, Long> endOffsets
    ) {
        Map<TopicPartition, Long> counts = new LinkedHashMap<>();

        List<TopicPartition> ordered = new ArrayList<>(endOffsets.keySet());
        ordered.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

        for (TopicPartition tp : ordered) {
            long begin = beginOffsets.getOrDefault(tp, 0L);
            long end = endOffsets.getOrDefault(tp, 0L);
            counts.put(tp, Math.max(0L, end - begin));
        }

        return counts;
    }

    private void printPreCopyStats(Map<TopicPartition, Long> partitionCounts) {
        out("====================================================");
        out("PRE-COPY STATS (from source Kafka offset snapshot)");
        out("====================================================");

        Map<String, Long> topicTotals = sumByTopic(partitionCounts);

        for (Map.Entry<String, Long> topicEntry : topicTotals.entrySet()) {
            out("Topic {}: {} message(s)", topicEntry.getKey(), topicEntry.getValue());

            partitionCounts.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topicEntry.getKey()))
                .forEach(e -> out("  Partition {}: {} message(s)", e.getKey().partition(), e.getValue()));
        }

        long grandTotal = topicTotals.values().stream().mapToLong(Long::longValue).sum();
        out("Total messages to copy: {}", grandTotal);
    }

    private void createTopicsOnTarget(AdminClient targetAdmin, Map<String, TopicDescription> sourceTopics) throws Exception {
        Set<String> existingTargetTopics = targetAdmin.listTopics(new ListTopicsOptions().listInternal(true)).names().get();

        List<NewTopic> toCreate = new ArrayList<>();
        for (Map.Entry<String, TopicDescription> entry : sourceTopics.entrySet()) {
            String topic = entry.getKey();
            if (existingTargetTopics.contains(topic)) {
                continue;
            }

            int partitionCount = entry.getValue().partitions().size();
            NewTopic newTopic = new NewTopic(topic, partitionCount, config.targetReplicationFactor);
            toCreate.add(newTopic);
        }

        if (toCreate.isEmpty()) {
            out("All topics already exist on target. No topic creation needed.");
            return;
        }

        out("Creating {} topic(s) on target KOF", toCreate.size());

        try {
            CreateTopicsResult result = targetAdmin.createTopics(toCreate);
            result.all().get();
            out("Topic creation completed on target.");
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicExistsException) {
                logger.warn("Some topics already existed while creating on target. Continuing.");
                return;
            }
            throw e;
        }
    }

    private Map<TopicPartition, Long> replicateSnapshot(
        KafkaConsumer<byte[], byte[]> sourceConsumer,
        KafkaProducer<byte[], byte[]> targetProducer,
        Map<TopicPartition, Long> beginOffsets,
        Map<TopicPartition, Long> endOffsets
    ) {
        List<TopicPartition> partitions = new ArrayList<>(endOffsets.keySet());
        partitions.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

        sourceConsumer.assign(partitions);
        for (TopicPartition tp : partitions) {
            sourceConsumer.seek(tp, beginOffsets.getOrDefault(tp, 0L));
        }

        Map<TopicPartition, Long> actualReadCounts = new HashMap<>();
        Set<TopicPartition> completed = new HashSet<>();

        for (TopicPartition tp : partitions) {
            long begin = beginOffsets.getOrDefault(tp, 0L);
            long end = endOffsets.getOrDefault(tp, 0L);
            if (begin >= end) {
                completed.add(tp);
            }
        }

        long publishedTotal = 0L;

        // Process partitions in batches of batchSize offsets at a time.
        // Each iteration assigns the next window [batchStart, batchEnd) per partition,
        // polls until the window is drained, flushes to KOF, then advances the window.
        Map<TopicPartition, Long> batchStart = new HashMap<>(beginOffsets);

        while (completed.size() < partitions.size()) {
            // Build the window for this batch
            Map<TopicPartition, Long> batchEnd = new HashMap<>();
            List<TopicPartition> activeBatch = new ArrayList<>();
            for (TopicPartition tp : partitions) {
                if (completed.contains(tp)) continue;
                long start = batchStart.getOrDefault(tp, 0L);
                long end   = endOffsets.getOrDefault(tp, 0L);
                long windowEnd = Math.min(start + config.batchSize, end);
                batchEnd.put(tp, windowEnd);
                activeBatch.add(tp);
            }

            if (activeBatch.isEmpty()) break;

            // Seek each partition to its batch start
            sourceConsumer.assign(activeBatch);
            for (TopicPartition tp : activeBatch) {
                sourceConsumer.seek(tp, batchStart.getOrDefault(tp, 0L));
            }

            Set<TopicPartition> batchDone = new HashSet<>();
            int emptyPolls = 0;

            while (batchDone.size() < activeBatch.size()) {
                ConsumerRecords<byte[], byte[]> records = sourceConsumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    emptyPolls++;
                    for (TopicPartition tp : activeBatch) {
                        if (batchDone.contains(tp)) continue;
                        long position = sourceConsumer.position(tp);
                        if (position >= batchEnd.getOrDefault(tp, 0L)) {
                            batchDone.add(tp);
                        }
                    }
                    if (emptyPolls >= config.maxEmptyPolls) {
                        throw new KafkaException(
                            "Reached max empty polls during batch replication. " +
                            "Increase max.empty.polls if source brokers are slow."
                        );
                    }
                    continue;
                }

                emptyPolls = 0;

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    long windowEnd = batchEnd.getOrDefault(tp, 0L);
                    long globalEnd = endOffsets.getOrDefault(tp, 0L);

                    if (record.offset() >= windowEnd) {
                        batchDone.add(tp);
                        continue;
                    }

                    ProducerRecord<byte[], byte[]> out = new ProducerRecord<>(
                        record.topic(),
                        record.partition(),
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        record.headers()
                    );

                    targetProducer.send(out, (metadata, ex) -> {
                        if (ex != null) {
                            System.out.println("ERROR: Send failed for " + record.topic() + "-" + record.partition() + ": " + ex.getMessage());
                            System.out.flush();
                        }
                    });
                    publishedTotal++;
                    actualReadCounts.merge(tp, 1L, Long::sum);

                    if (record.offset() + 1 >= windowEnd) {
                        batchDone.add(tp);
                    }

                    // Mark globally complete if we reached the real end
                    if (record.offset() + 1 >= globalEnd) {
                        completed.add(tp);
                    }
                }
            }

            // Flush batch to KOF before moving to next window
            targetProducer.flush();
            // Log per-topic progress for this batch
            StringBuilder batchSummary = new StringBuilder();
            for (TopicPartition tp : activeBatch) {
                long sent = actualReadCounts.getOrDefault(tp, 0L);
                long total = endOffsets.getOrDefault(tp, 0L) - beginOffsets.getOrDefault(tp, 0L);
                batchSummary.append(String.format("  %s-%d: %d/%d%n", tp.topic(), tp.partition(), sent, total));
            }
            out("Replicated {} message(s) total so far", publishedTotal);
            out(batchSummary.toString().trim());

            // Advance batch windows
            for (TopicPartition tp : activeBatch) {
                long nextStart = batchEnd.getOrDefault(tp, 0L);
                long globalEnd = endOffsets.getOrDefault(tp, 0L);
                batchStart.put(tp, nextStart);
                if (nextStart >= globalEnd) {
                    completed.add(tp);
                }
            }
        }

        out("Replicated {} message(s) to target KOF", publishedTotal);

        return actualReadCounts;
    }

    private void printFinalStats(
        Map<TopicPartition, Long> plannedCounts,
        Map<TopicPartition, Long> actualReadCounts
    ) {
        out("====================================================");
        out("POST-COPY STATS");
        out("====================================================");

        Map<String, Long> topicPlanned = sumByTopic(plannedCounts);
        Map<String, Long> topicActual = sumByTopic(actualReadCounts);

        Set<String> orderedTopics = new LinkedHashSet<>();
        orderedTopics.addAll(topicPlanned.keySet());
        orderedTopics.addAll(topicActual.keySet());

        for (String topic : orderedTopics) {
            long planned = topicPlanned.getOrDefault(topic, 0L);
            long actual = topicActual.getOrDefault(topic, 0L);
            out("Topic {}: planned={} published={}", topic, planned, actual);

            List<TopicPartition> partitions = plannedCounts.keySet().stream()
                .filter(tp -> tp.topic().equals(topic))
                .sorted(Comparator.comparingInt(TopicPartition::partition))
                .collect(Collectors.toList());

            for (TopicPartition tp : partitions) {
                long partitionPlanned = plannedCounts.getOrDefault(tp, 0L);
                long partitionActual = actualReadCounts.getOrDefault(tp, 0L);
                out("  Partition {}: planned={} published={}", tp.partition(), partitionPlanned, partitionActual);
            }
        }

        long plannedTotal = plannedCounts.values().stream().mapToLong(Long::longValue).sum();
        long actualTotal = actualReadCounts.values().stream().mapToLong(Long::longValue).sum();

        out("TOTAL planned={} published={}", plannedTotal, actualTotal);
    }

    private Map<String, Long> sumByTopic(Map<TopicPartition, Long> partitionCounts) {
        Map<String, Long> totals = new LinkedHashMap<>();

        partitionCounts.entrySet().stream()
            .sorted(Map.Entry.comparingByKey(Comparator
                .comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition)))
            .forEach(entry -> totals.merge(entry.getKey().topic(), entry.getValue(), Long::sum));

        return totals;
    }

    private static final class Config {
        private static final String DEFAULT_CONFIG_PATH = "conf/kafka-to-kof.properties";

        private final String sourceBootstrapServers;
        private final String targetBootstrapServers;
        private final Pattern topicPattern;
        private final boolean includeInternalTopics;
        private final short targetReplicationFactor;
        private final String clientId;
        private final int requestTimeoutMs;
        private final int maxEmptyPolls;
        private final int batchSize;
        private final boolean dryRun;
        private final boolean helpRequested;
        private final Properties rawProperties;

        private Config(
            String sourceBootstrapServers,
            String targetBootstrapServers,
            Pattern topicPattern,
            boolean includeInternalTopics,
            short targetReplicationFactor,
            String clientId,
            int requestTimeoutMs,
            int maxEmptyPolls,
            int batchSize,
            boolean dryRun,
            boolean helpRequested,
            Properties rawProperties
        ) {
            this.sourceBootstrapServers = sourceBootstrapServers;
            this.targetBootstrapServers = targetBootstrapServers;
            this.topicPattern = topicPattern;
            this.includeInternalTopics = includeInternalTopics;
            this.targetReplicationFactor = targetReplicationFactor;
            this.clientId = clientId;
            this.requestTimeoutMs = requestTimeoutMs;
            this.maxEmptyPolls = maxEmptyPolls;
            this.batchSize = batchSize;
            this.dryRun = dryRun;
            this.helpRequested = helpRequested;
            this.rawProperties = rawProperties;
        }

        static Config fromArgs(String[] args) throws IOException {
            Map<String, String> cli = parseCli(args);
            boolean helpRequested = cli.containsKey("help");

            if (helpRequested) {
                return new Config(
                    "",
                    "",
                    Pattern.compile(".*"),
                    false,
                    (short) 1,
                    "kafka-to-kof-replicator",
                    60000,
                    15,
                    1000,
                    false,
                    true,
                    new Properties()
                );
            }

            String configPath = cli.getOrDefault("config", DEFAULT_CONFIG_PATH);
            Properties props = new Properties();
            try (FileInputStream in = new FileInputStream(configPath)) {
                props.load(in);
            }

            String sourceBootstrap = cli.getOrDefault(
                "source-bootstrap",
                props.getProperty("source.bootstrap.servers", "localhost:9092")
            );
            String targetBootstrap = cli.getOrDefault(
                "target-bootstrap",
                props.getProperty("target.bootstrap.servers", "localhost:9093")
            );

            String topicPatternValue = cli.getOrDefault(
                "topic-pattern",
                props.getProperty("topic.pattern", ".*")
            );

            boolean includeInternal = Boolean.parseBoolean(cli.getOrDefault(
                "include-internal",
                props.getProperty("include.internal", "false")
            ));

            short replicationFactor = Short.parseShort(cli.getOrDefault(
                "replication-factor",
                props.getProperty("target.replication.factor", "1")
            ));

            String clientId = cli.getOrDefault(
                "client-id",
                props.getProperty("client.id", "kafka-to-kof-replicator")
            );

            int requestTimeoutMs = Integer.parseInt(cli.getOrDefault(
                "request-timeout-ms",
                props.getProperty("request.timeout.ms", "60000")
            ));

            int maxEmptyPolls = Integer.parseInt(cli.getOrDefault(
                "max-empty-polls",
                props.getProperty("max.empty.polls", "15")
            ));

            int batchSize = Integer.parseInt(cli.getOrDefault(
                "batch-size",
                props.getProperty("batch.size", "1000")
            ));

            if (batchSize <= 0) {
                throw new IllegalArgumentException("batch.size must be > 0");
            }

            boolean dryRun = cli.containsKey("dry-run") || Boolean.parseBoolean(
                props.getProperty("dry.run", "false")
            );

            if (sourceBootstrap.isBlank()) {
                throw new IllegalArgumentException("source.bootstrap.servers cannot be empty");
            }
            if (targetBootstrap.isBlank()) {
                throw new IllegalArgumentException("target.bootstrap.servers cannot be empty");
            }
            if (replicationFactor <= 0) {
                throw new IllegalArgumentException("target.replication.factor must be > 0");
            }
            if (maxEmptyPolls <= 0) {
                throw new IllegalArgumentException("max.empty.polls must be > 0");
            }

            return new Config(
                sourceBootstrap,
                targetBootstrap,
                Pattern.compile(topicPatternValue),
                includeInternal,
                replicationFactor,
                clientId,
                requestTimeoutMs,
                maxEmptyPolls,
                batchSize,
                dryRun,
                false,
                props
            );
        }

        private static Map<String, String> parseCli(String[] args) {
            Map<String, String> cli = new HashMap<>();

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];

                switch (arg) {
                    case "--help":
                    case "-h":
                        cli.put("help", "true");
                        break;
                    case "--dry-run":
                        cli.put("dry-run", "true");
                        break;
                    case "--config":
                    case "--source-bootstrap":
                    case "--target-bootstrap":
                    case "--topic-pattern":
                    case "--include-internal":
                    case "--replication-factor":
                    case "--client-id":
                    case "--request-timeout-ms":
                    case "--batch-size":
                    case "--max-empty-polls":
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException("Missing value for argument: " + arg);
                        }
                        cli.put(arg.substring(2), args[++i]);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            return cli;
        }

        Properties buildSourceAdminProperties() {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrapServers);
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId + "-source-admin");
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
            copyPrefixed(rawProperties, props, "source.admin.");
            return props;
        }

        Properties buildTargetAdminProperties() {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, targetBootstrapServers);
            props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId + "-target-admin");
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
            copyPrefixed(rawProperties, props, "target.admin.");
            return props;
        }

        Properties buildSourceConsumerProperties() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceBootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, clientId + "-snapshot-reader");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId + "-source-consumer");
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
            copyPrefixed(rawProperties, props, "source.consumer.");
            return props;
        }

        Properties buildTargetProducerProperties() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, targetBootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.RETRIES_CONFIG, "3");
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
            props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId + "-target-producer");
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs));
            copyPrefixed(rawProperties, props, "target.producer.");
            return props;
        }

        private static void copyPrefixed(Properties source, Properties target, String prefix) {
            for (String key : source.stringPropertyNames()) {
                if (key.startsWith(prefix)) {
                    String stripped = key.substring(prefix.length());
                    if (!stripped.isEmpty()) {
                        target.put(stripped, source.getProperty(key));
                    }
                }
            }
        }

        static void printUsage() {
            System.out.println("KafkaToKofReplicatorApp usage:");
            System.out.println("  java -cp <classpath> com.tibco.ftl.kof.KafkaToKofReplicatorApp [options]");
            System.out.println();
            System.out.println("Options:");
            System.out.println("  --config <path>            Path to properties file (default: conf/kafka-to-kof.properties)");
            System.out.println("  --source-bootstrap <list>  Source Kafka bootstrap servers (overrides config)");
            System.out.println("  --target-bootstrap <list>  Target KOF bootstrap servers (overrides config)");
            System.out.println("  --topic-pattern <regex>    Topic name regex filter (overrides config)");
            System.out.println("  --include-internal <bool>  Include internal topics (overrides config)");
            System.out.println("  --replication-factor <n>   Target topic replication factor (overrides config)");
            System.out.println("  --client-id <id>           Client id prefix (overrides config)");
            System.out.println("  --request-timeout-ms <ms>  Request timeout in milliseconds (overrides config)");
            System.out.println("  --batch-size <n>           Offsets per batch per partition (default 1000)");
            System.out.println("  --max-empty-polls <n>      Max consecutive empty polls before failing");
            System.out.println("  --dry-run                  Print stats only, skip topic create + publish");
            System.out.println("  --help                     Print this message");
            System.out.println();
            System.out.println("Config file keys:");
            System.out.println("  source.bootstrap.servers");
            System.out.println("  target.bootstrap.servers");
            System.out.println("  topic.pattern");
            System.out.println("  include.internal");
            System.out.println("  target.replication.factor");
            System.out.println("  client.id");
            System.out.println("  request.timeout.ms");
            System.out.println("  batch.size (default 1000)");
            System.out.println("  max.empty.polls");
            System.out.println("  dry.run");
            System.out.println("  source.admin.<kafka.property>");
            System.out.println("  source.consumer.<kafka.property>");
            System.out.println("  target.admin.<kafka.property>");
            System.out.println("  target.producer.<kafka.property>");
        }
    }
}
