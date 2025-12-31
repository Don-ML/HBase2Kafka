package com.dll;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class KafkaReplicationEndpoint extends BaseReplicationEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaReplicationEndpoint.class);

    private volatile KafkaProducer<byte[], byte[]> producer;
    private volatile boolean started;
    private volatile UUID peerUUID;

    private String bootstrapServers;
    private String fixedTopic;
    private String topicPrefix;
    private long sendTimeoutMs;

    @Override
    public void init(Context context) throws IOException {
        super.init(context);
        Configuration conf = context.getConfiguration();
        this.bootstrapServers = trimToNull(conf.get("replication.kafka.bootstrap.servers"));
        if (this.bootstrapServers == null) {
            throw new IOException("Missing required config: replication.kafka.bootstrap.servers");
        }
        this.fixedTopic = trimToNull(conf.get("replication.kafka.topic"));
        this.topicPrefix = conf.get("replication.kafka.topic.prefix", "hbase-repl-");
        this.sendTimeoutMs = conf.getLong("replication.kafka.send.timeout.ms", 30000L);
        String identity = this.bootstrapServers + "|" + Objects.toString(this.fixedTopic, "") + "|" + Objects.toString(this.topicPrefix, "");
        this.peerUUID = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public UUID getPeerUUID() {
        return peerUUID;
    }

    @Override
    public void start() {
        startAsync();
    }

    @Override
    public void stop() {
        stopAsync();
    }

    @Override
    protected void doStart() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
        this.started = true;
        notifyStarted();
    }

    @Override
    protected void doStop() {
        KafkaProducer<byte[], byte[]> p = this.producer;
        this.producer = null;
        this.started = false;
        if (p != null) {
            try {
                p.flush();
                p.close(Duration.ofSeconds(30));
            } catch (Exception e) {
                LOG.warn("Failed to close Kafka producer", e);
            }
        }
        notifyStopped();
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
        if (!started) {
            return false;
        }
        KafkaProducer<byte[], byte[]> p = this.producer;
        if (p == null) {
            return false;
        }

        List<WAL.Entry> entries = replicateContext.getEntries();
        if (entries == null || entries.isEmpty()) {
            return true;
        }

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (WAL.Entry entry : entries) {
            if (entry == null || entry.getEdit() == null) {
                continue;
            }
            TableName tableName = entry.getKey() == null ? null : entry.getKey().getTableName();
            if (tableName == null) {
                continue;
            }
            String topic = topicForTable(tableName);
            List<Cell> cells = entry.getEdit().getCells();
            if (cells == null || cells.isEmpty()) {
                continue;
            }
            for (Cell cell : cells) {
                if (cell == null) {
                    continue;
                }
                byte[] key = CellUtil.cloneRow(cell);
                byte[] value = buildJsonBytes(tableName, cell);
                futures.add(p.send(new ProducerRecord<>(topic, key, value)));
            }
        }

        if (futures.isEmpty()) {
            return true;
        }

        try {
            long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sendTimeoutMs);
            for (Future<RecordMetadata> f : futures) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new IOException("Kafka send timed out");
                }
                f.get(remainingNanos, TimeUnit.NANOSECONDS);
            }
            return true;
        } catch (Exception e) {
            LOG.warn("Replication to Kafka failed; entries={}", entries.size(), e);
            return false;
        }
    }

    private String topicForTable(TableName tableName) {
        if (fixedTopic != null) {
            return fixedTopic;
        }
        String raw = tableName.getNameAsString();
        String normalized = raw.replace(':', '_').replace('/', '_');
        return topicPrefix + normalized;
    }

    private static byte[] buildJsonBytes(TableName tableName, Cell cell) {
        String table = tableName.getNameAsString();
        String row = Base64.getEncoder().encodeToString(CellUtil.cloneRow(cell));
        String family = Base64.getEncoder().encodeToString(CellUtil.cloneFamily(cell));
        String qualifier = Base64.getEncoder().encodeToString(CellUtil.cloneQualifier(cell));
        String value = Base64.getEncoder().encodeToString(CellUtil.cloneValue(cell));
        long ts = cell.getTimestamp();
        int type = cell.getTypeByte();

        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        appendJsonField(sb, "table", table);
        sb.append(',');
        appendJsonField(sb, "row_b64", row);
        sb.append(',');
        appendJsonField(sb, "family_b64", family);
        sb.append(',');
        appendJsonField(sb, "qualifier_b64", qualifier);
        sb.append(',');
        appendJsonField(sb, "value_b64", value);
        sb.append(",\"ts\":").append(ts);
        sb.append(",\"type\":").append(type);
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendJsonField(StringBuilder sb, String key, String value) {
        sb.append('"').append(escapeJson(Objects.requireNonNull(key))).append('"').append(':');
        if (value == null) {
            sb.append("null");
            return;
        }
        sb.append('"').append(escapeJson(value)).append('"');
    }

    private static String escapeJson(String s) {
        StringBuilder out = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    out.append("\\\"");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '\b':
                    out.append("\\b");
                    break;
                case '\f':
                    out.append("\\f");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.toString();
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
