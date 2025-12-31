# KafkaReplicationEndpoint（HBase Replication -> Kafka）
简介：
 本文档说明怎么实时读取HBASE数据写到Kafka 给后续实时ETL做准备
 以及 [KafkaReplicationEndpoint.java]的实现逻辑、部署方式，以及在 HBase 侧开启复制并验证写入 Kafka 的完整步骤。
技术架构：
 使用HBASE WAL日志数据实时增量推送至Kafka


## 1. 这个 Endpoint 做了什么

- 运行位置：HBase RegionServer 进程内（ReplicationSource 调用 ReplicationEndpoint）。
- 输入：HBase WAL 批次（`List<WAL.Entry>`），每个 entry 内包含若干 `Cell`。
- 输出：把每个 `Cell` 写到 Kafka（ProducerRecord）。
  - key：`rowkey`（byte[]，用于同一 row 的消息尽量落同一分区）
  - value：JSON（UTF-8），其中 row/family/qualifier/value 都用 Base64 编码字段承载，避免二进制/不可见字符问题。

### 1.1 Topic 规则

- 如果配置了 `replication.kafka.topic`：所有表都写入这个固定 topic。
- 否则：按表分 topic：`replication.kafka.topic.prefix + <tableName>`
  - `tableName` 是 `namespace:table` 的字符串形式
  - `:` 和 `/` 会被替换为 `_`
  - 示例：`test:test` -> `hbase-repl-test_test`

## 2. Kafka 消息格式

value 示例（字段含义见下表）：

```json
{
  "table":"test:test",
  "row_b64":"cm93MQ==",
  "family_b64":"Y2Y=",
  "qualifier_b64":"cTE=",
  "value_b64":"djE=",
  "ts":1735650000000,
  "type":4
}
```

字段说明：

| 字段 | 含义 |
|---|---|
| table | 表名（namespace:table） |
| row_b64 | rowkey（Base64） |
| family_b64 | 列族（Base64） |
| qualifier_b64 | 列限定符（Base64） |
| value_b64 | value（Base64） |
| ts | cell 时间戳 |
| type | cell type（不同 HBase 版本含义略有差别，用于区分 put/delete 等） |

## 3. Endpoint 配置项（HBase 侧）

这些配置项通过 `add_peer ... CONFIG => {...}` 下发给 Endpoint：

- 必填
  - `replication.kafka.bootstrap.servers`：Kafka bootstrap servers，例如 `localhost:9092`
- 可选
  - `replication.kafka.topic`：固定 topic（配置后不按表分 topic）
  - `replication.kafka.topic.prefix`：按表分 topic 的前缀，默认 `hbase-repl-`
  - `replication.kafka.send.timeout.ms`：发送/等待超时，默认 `30000`

## 4. 构建产物（重要：不要把 fat jar 放进 HBase lib）

本项目使用了 shade 打包，同时输出两个 jar：

- 瘦 jar（给 HBase RegionServer/lib 使用）：`target/HBase2Kafka-1.0-SNAPSHOT.jar`
- fat jar（带依赖，给 Flink 作业等使用）：`target/HBase2Kafka-1.0-SNAPSHOT-all.jar`

必须把「瘦 jar」放进 HBase 的 `lib/`。如果把 fat jar 放进 HBase 的 `lib/`，可能会污染 HBase 自己的类/资源加载（例如导致 `hbase shell` 启动时报 `HTableDescriptor` 缺失等）。

构建命令：

```bash
cd HBase2Kafka
mvn -DskipTests package
```

## 5. 部署到 HBase（2.5.13）一步一步

以下步骤以你本机路径为例：

- HBase：`hbase-2.5.13`
- Kafka：`kafka_2.13-3.9.1`

### 5.1 拷贝 jar 到 RegionServer

1）把 Endpoint 所在的瘦 jar 拷到 HBase lib：

```bash
cp HBase2Kafka/target/HBase2Kafka-1.0-SNAPSHOT.jar \
  /hbase-2.5.13/lib/
```

2）补齐 Kafka Producer 运行时依赖（最小化依赖，避免冲突）：

```bash
cp /kafka_2.13-3.9.1/libs/kafka-clients-3.9.1.jar \
  /hbase-2.5.13/lib/
```

3）重启 HBase（至少重启 RegionServer 进程）让 classpath 生效。

### 5.2 HBase 开启复制（集群级）

确认 `hbase-site.xml` 内开启（一般默认即可）：

- `hbase.replication = true`

改完需要重启 HBase。

### 5.3 创建 Replication Peer（指向 Kafka Endpoint）

进入 HBase shell：

```bash
/hbase-2.5.13/bin/hbase shell
```

创建 peer：

```ruby
add_peer 'kafkaPeers',
  ENDPOINT_CLASSNAME => 'com.dll.KafkaReplicationEndpoint',
  CONFIG => {
    'replication.kafka.bootstrap.servers' => 'localhost:9092',
    'replication.kafka.topic.prefix' => 'hbase_',
    'replication.kafka.send.timeout.ms' => '30000'
  }

list_peers
```

### 5.4 对目标表“所有列族”开启复制

HBase 的复制开关是按列族生效的。你需要把每个列族的 `REPLICATION_SCOPE` 设为 `1`。

1）如果 namespace 不存在，先创建：

```ruby
create_namespace 'test'
list_namespace
```

2）创建一个示例表（两个列族都开启复制）：

```ruby
create 'test:test',
  { NAME => 'cf',  REPLICATION_SCOPE => 1 },
  { NAME => 'cf2', REPLICATION_SCOPE => 1 }
```

3）如果是已有表：先查看列族，再逐个开启（示例以 `cf`、`cf2` 两个列族为例）：

```ruby
describe 'test:test'

disable 'test:test'
alter 'test:test', { NAME => 'cf',  REPLICATION_SCOPE => 1 }
alter 'test:test', { NAME => 'cf2', REPLICATION_SCOPE => 1 }
enable 'test:test'
```

## 6. 验证（写 HBase -> 看 Kafka）

### 6.1 在 HBase 写入/删除

```ruby
put 'test:test', 'row1', 'cf:q1',  'v1'
put 'test:test', 'row1', 'cf2:q2', 'v2'
delete 'test:test', 'row1', 'cf:q1'
```

### 6.2 直接消费 Kafka topic

默认按表分 topic：`hbase_test_test`（`test:test` 中的 `:` 会替换成 `_`）：

```bash
/Users/dll/DLL/flink_iceberg/.local/kafka_2.13-3.9.1/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hbase_test_test \
  --from-beginning
```

如果你配置了 `replication.kafka.topic`（固定 topic），则把上面 `--topic` 改为你的固定 topic 即可。

## 7. 常见问题

### 7.1 `Unknown namespace xxx!`

表示你使用了 `xxx:table`，但 `xxx` namespace 不存在。

```ruby
create_namespace 'xxx'
```

### 7.2 往 HBase lib 放 jar 后 `hbase shell` 启动报 `HTableDescriptor` 缺失

常见原因是把 fat jar 放进了 HBase 的 `lib/`，污染了 HBase 的类/资源加载。请只放瘦 jar：

- 放：`HBase2Kafka-1.0-SNAPSHOT.jar`
- 不要放：`HBase2Kafka-1.0-SNAPSHOT-all.jar`
