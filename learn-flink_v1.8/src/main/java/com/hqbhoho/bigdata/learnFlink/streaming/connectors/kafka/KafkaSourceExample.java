package com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka;

import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.User;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Properties;

/**
 * describe:
 * Kafka Source Example
 * AvroDeserializationSchema.forSpecific(...)   This deserialization schema expects that the serialized records DO NOT contain embedded schema.
 * Kafka message use avro serializer and deserializer
 * <p>
 * message value format: {"id": 106, "name": "user---106", "items": [{"id": 106, "name": "item---106", "price": 106.99}]}
 * <p>
 * ******************************************************************************************************************************************************************
 * Test 1:
 * env.setParallelism(1);
 * consumer.assignTimestampsAndWatermarks(new KafkaMessageAssignTimestampAndWatermarks());
 * <p>
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 0
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 34, CreateTime = 1558070764289, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 100, value = {"id": 100, "name": "user---100", "items": [{"id": 100, "name": "item---100", "price": 109.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 44, CreateTime = 1558070765112, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 101, value = {"id": 101, "name": "user---101", "items": [{"id": 101, "name": "item---101", "price": 110.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 35, CreateTime = 1558070765614, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 102, value = {"id": 102, "name": "user---102", "items": [{"id": 102, "name": "item---102", "price": 111.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 45, CreateTime = 1558070766115, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 103, value = {"id": 103, "name": "user---103", "items": [{"id": 103, "name": "item---103", "price": 112.9}]})
 * Thread id: 57,watermater generate: 1558070766115
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 1558070765614
 * 2019-05-17 13:26:06.266 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 2 @ 1558070766266 for job 9f1984133e4dda9e9a372d728e29df72.
 * 2019-05-17 13:26:06.273 [flink-akka.actor.default-dispatcher-7] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 2 for job 9f1984133e4dda9e9a372d728e29df72 (3060 bytes in 5 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 46, CreateTime = 1558070766616, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 104, value = {"id": 104, "name": "user---104", "items": [{"id": 104, "name": "item---104", "price": 113.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 47, CreateTime = 1558070767118, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 105, value = {"id": 105, "name": "user---105", "items": [{"id": 105, "name": "item---105", "price": 114.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 36, CreateTime = 1558070767619, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 106, value = {"id": 106, "name": "user---106", "items": [{"id": 106, "name": "item---106", "price": 115.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 37, CreateTime = 1558070768120, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 107, value = {"id": 107, "name": "user---107", "items": [{"id": 107, "name": "item---107", "price": 116.9}]})
 * Thread id: 57,watermater generate: 1558070767118
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 1558070768120
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 38, CreateTime = 1558070768622, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 108, value = {"id": 108, "name": "user---108", "items": [{"id": 108, "name": "item---108", "price": 117.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 39, CreateTime = 1558070769124, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 109, value = {"id": 109, "name": "user---109", "items": [{"id": 109, "name": "item---109", "price": 118.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 17, CreateTime = 1558070769625, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 110, value = {"id": 110, "name": "user---110", "items": [{"id": 110, "name": "item---110", "price": 119.9}]})
 * Thread id: 57,watermater generate: 1558070770126
 * Thread id: 57,watermater generate: 1558070769625
 * Thread id: 57,watermater generate: 1558070769124
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 48, CreateTime = 1558070770126, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 111, value = {"id": 111, "name": "user---111", "items": [{"id": 111, "name": "item---111", "price": 120.9}]})
 * (1,109.9,1558070760000,1558070765000)
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 49, CreateTime = 1558070770627, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 112, value = {"id": 112, "name": "user---112", "items": [{"id": 112, "name": "item---112", "price": 121.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 18, CreateTime = 1558070771128, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 113, value = {"id": 113, "name": "user---113", "items": [{"id": 113, "name": "item---113", "price": 122.9}]})
 * 2019-05-17 13:26:11.274 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 3 @ 1558070771274 for job 9f1984133e4dda9e9a372d728e29df72.
 * 2019-05-17 13:26:11.288 [flink-akka.actor.default-dispatcher-3] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 3 for job 9f1984133e4dda9e9a372d728e29df72 (3060 bytes in 13 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 50, CreateTime = 1558070771629, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 114, value = {"id": 114, "name": "user---114", "items": [{"id": 114, "name": "item---114", "price": 123.9}]})
 * Thread id: 57,watermater generate: 1558070771629
 * Thread id: 57,watermater generate: 1558070772130
 * Thread id: 57,watermater generate: 1558070769124
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 19, CreateTime = 1558070772130, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 115, value = {"id": 115, "name": "user---115", "items": [{"id": 115, "name": "item---115", "price": 124.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 51, CreateTime = 1558070772634, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 116, value = {"id": 116, "name": "user---116", "items": [{"id": 116, "name": "item---116", "price": 125.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 52, CreateTime = 1558070773136, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 117, value = {"id": 117, "name": "user---117", "items": [{"id": 117, "name": "item---117", "price": 126.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 53, CreateTime = 1558070773637, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 118, value = {"id": 118, "name": "user---118", "items": [{"id": 118, "name": "item---118", "price": 127.9}]})
 * Thread id: 57,watermater generate: 1558070773637
 * Thread id: 57,watermater generate: 1558070772130
 * Thread id: 57,watermater generate: 1558070769124
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 40, CreateTime = 1558070774139, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 119, value = {"id": 119, "name": "user---119", "items": [{"id": 119, "name": "item---119", "price": 128.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 20, CreateTime = 1558070774641, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 120, value = {"id": 120, "name": "user---120", "items": [{"id": 120, "name": "item---120", "price": 129.9}]})
 * Thread id: 57,watermater generate: 1558070773637
 * Thread id: 57,watermater generate: 1558070774641
 * Thread id: 57,watermater generate: 1558070774139
 * (10,1154.0,1558070765000,1558070770000)
 * <p>
 * watermark生成与partition有关   3个partition  产生三个序列的watermark   处理过程有点像  env.setParallelism(3)   watermark应该和partition信息有关
 * 以分区为单位，批量处理event,生成watermark
 * 源码：
 * {@link org.apache.flink.streaming.connectors.kafka.internal.Kafka09Fetcher#runFetchLoop()}
 * ********************************************************************************************************************************************************
 * Test 2 :
 * env.setParallelism(1);
 * inputStream.assignTimestampsAndWatermarks(new KafkaMessageAssignTimestampAndWatermarks())
 * <p>
 * Thread id: 57,watermater generate: 0
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 41, CreateTime = 1558074260773, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 100, value = {"id": 100, "name": "user---100", "items": [{"id": 100, "name": "item---100", "price": 109.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 54, CreateTime = 1558074261522, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 101, value = {"id": 101, "name": "user---101", "items": [{"id": 101, "name": "item---101", "price": 110.9}]})
 * Thread id: 57,watermater generate: 1558074261522
 * 2019-05-17 14:24:21.991 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 4 @ 1558074261991 for job 74cd0b6778183122cf04b39c66bd9974.
 * 2019-05-17 14:24:22.001 [flink-akka.actor.default-dispatcher-3] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 4 for job 74cd0b6778183122cf04b39c66bd9974 (3006 bytes in 10 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 42, CreateTime = 1558074262024, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 102, value = {"id": 102, "name": "user---102", "items": [{"id": 102, "name": "item---102", "price": 111.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 55, CreateTime = 1558074262525, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 103, value = {"id": 103, "name": "user---103", "items": [{"id": 103, "name": "item---103", "price": 112.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 56, CreateTime = 1558074263026, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 104, value = {"id": 104, "name": "user---104", "items": [{"id": 104, "name": "item---104", "price": 113.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 57, CreateTime = 1558074263527, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 105, value = {"id": 105, "name": "user---105", "items": [{"id": 105, "name": "item---105", "price": 114.9}]})
 * Thread id: 57,watermater generate: 1558074263527
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 43, CreateTime = 1558074264031, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 106, value = {"id": 106, "name": "user---106", "items": [{"id": 106, "name": "item---106", "price": 115.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 44, CreateTime = 1558074264533, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 107, value = {"id": 107, "name": "user---107", "items": [{"id": 107, "name": "item---107", "price": 116.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 45, CreateTime = 1558074265033, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 108, value = {"id": 108, "name": "user---108", "items": [{"id": 108, "name": "item---108", "price": 117.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 46, CreateTime = 1558074265534, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 109, value = {"id": 109, "name": "user---109", "items": [{"id": 109, "name": "item---109", "price": 118.9}]})
 * Thread id: 57,watermater generate: 1558074265534
 * (8,907.1999999999999,1558074260000,1558074265000)
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 21, CreateTime = 1558074266035, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 110, value = {"id": 110, "name": "user---110", "items": [{"id": 110, "name": "item---110", "price": 119.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 58, CreateTime = 1558074266536, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 111, value = {"id": 111, "name": "user---111", "items": [{"id": 111, "name": "item---111", "price": 120.9}]})
 * 2019-05-17 14:24:27.001 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 5 @ 1558074267001 for job 74cd0b6778183122cf04b39c66bd9974.
 * 2019-05-17 14:24:27.012 [flink-akka.actor.default-dispatcher-7] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 5 for job 74cd0b6778183122cf04b39c66bd9974 (3006 bytes in 11 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 59, CreateTime = 1558074267036, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 112, value = {"id": 112, "name": "user---112", "items": [{"id": 112, "name": "item---112", "price": 121.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 22, CreateTime = 1558074267538, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 113, value = {"id": 113, "name": "user---113", "items": [{"id": 113, "name": "item---113", "price": 122.9}]})
 * Thread id: 57,watermater generate: 1558074267538
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 60, CreateTime = 1558074268038, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 114, value = {"id": 114, "name": "user---114", "items": [{"id": 114, "name": "item---114", "price": 123.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 23, CreateTime = 1558074268538, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 115, value = {"id": 115, "name": "user---115", "items": [{"id": 115, "name": "item---115", "price": 124.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 61, CreateTime = 1558074269041, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 116, value = {"id": 116, "name": "user---116", "items": [{"id": 116, "name": "item---116", "price": 125.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 62, CreateTime = 1558074269542, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 117, value = {"id": 117, "name": "user---117", "items": [{"id": 117, "name": "item---117", "price": 126.9}]})
 * Thread id: 57,watermater generate: 1558074269542
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 63, CreateTime = 1558074270045, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 118, value = {"id": 118, "name": "user---118", "items": [{"id": 118, "name": "item---118", "price": 127.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 47, CreateTime = 1558074270546, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 119, value = {"id": 119, "name": "user---119", "items": [{"id": 119, "name": "item---119", "price": 128.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 24, CreateTime = 1558074271047, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 120, value = {"id": 120, "name": "user---120", "items": [{"id": 120, "name": "item---120", "price": 129.9}]})
 * Thread id: 57,watermater generate: 1558074271047
 * (10,1224.0,1558074265000,1558074270000)
 * <p>
 * 和之前基于（socket）的event time 处理场景一样，无特殊内容
 * *************************************************************************************************************************************************************************
 * Test3 : 模拟宕机
 * 2019-05-17 14:48:14.617 [Kafka 0.10 Fetcher for Source: kafka-source -> Timestamps/Watermarks (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka version : 0.11.0.2
 * 2019-05-17 14:48:14.617 [Kafka 0.10 Fetcher for Source: kafka-source -> Timestamps/Watermarks (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka commitId : 73be1e1168f91ee2
 * 2019-05-17 14:48:14.637 [Kafka 0.10 Fetcher for Source: kafka-source -> Timestamps/Watermarks (1/1)] INFO  o.a.k.c.c.i.AbstractCoordinator - Discovered coordinator 192.168.5.109:9092 (id: 2147483646 rack: null) for group hqbhoho0001.
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 0
 * 2019-05-17 14:48:19.194 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 1 @ 1558075699181 for job 483eda544eb25a74df79767bead0a81b.
 * 2019-05-17 14:48:19.495 [flink-akka.actor.default-dispatcher-3] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 1 for job 483eda544eb25a74df79767bead0a81b (2952 bytes in 313 ms).
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 0
 * 2019-05-17 14:48:24.497 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 2 @ 1558075704497 for job 483eda544eb25a74df79767bead0a81b.
 * 2019-05-17 14:48:24.504 [flink-akka.actor.default-dispatcher-5] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 2 for job 483eda544eb25a74df79767bead0a81b (2952 bytes in 6 ms).
 * Thread id: 57,watermater generate: 0
 * Thread id: 57,watermater generate: 0
 * 2019-05-17 14:48:29.508 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 3 @ 1558075709508 for job 483eda544eb25a74df79767bead0a81b.
 * 2019-05-17 14:48:29.518 [flink-akka.actor.default-dispatcher-7] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 3 for job 483eda544eb25a74df79767bead0a81b (2952 bytes in 9 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 25, CreateTime = 1558075709484, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 200, value = {"id": 200, "name": "user---200", "items": [{"id": 200, "name": "item---200", "price": 209.9}]})
 * Thread id: 57,watermater generate: 1558075709484
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 26, CreateTime = 1558075711672, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 201, value = {"id": 201, "name": "user---201", "items": [{"id": 201, "name": "item---201", "price": 210.9}]})
 * Thread id: 57,watermater generate: 1558075711672
 * (1,209.9,1558075705000,1558075710000)
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 64, CreateTime = 1558075713672, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 202, value = {"id": 202, "name": "user---202", "items": [{"id": 202, "name": "item---202", "price": 211.9}]})
 * Thread id: 57,watermater generate: 1558075713672
 * 2019-05-17 14:48:34.519 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 4 @ 1558075714519 for job 483eda544eb25a74df79767bead0a81b.
 * 2019-05-17 14:48:34.526 [flink-akka.actor.default-dispatcher-4] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 4 for job 483eda544eb25a74df79767bead0a81b (3006 bytes in 7 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 27, CreateTime = 1558075715673, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 203, value = {"id": 203, "name": "user---203", "items": [{"id": 203, "name": "item---203", "price": 212.9}]})
 * Thread id: 57,watermater generate: 1558075715673
 * (2,422.8,1558075710000,1558075715000)
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 48, CreateTime = 1558075717675, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 204, value = {"id": 204, "name": "user---204", "items": [{"id": 204, "name": "item---204", "price": 213.9}]})
 * Thread id: 57,watermater generate: 1558075717675
 * 2019-05-17 14:48:39.527 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 5 @ 1558075719527 for job 483eda544eb25a74df79767bead0a81b.
 * 2019-05-17 14:48:39.531 [flink-akka.actor.default-dispatcher-5] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 5 for job 483eda544eb25a74df79767bead0a81b (3006 bytes in 4 ms).
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 65, CreateTime = 1558075719676, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 205, value = {"id": 205, "name": "user---205", "items": [{"id": 205, "name": "item---205", "price": 214.9}]})
 * Thread id: 57,watermater generate: 1558075719676
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 49, CreateTime = 1558075721676, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 206, value = {"id": 206, "name": "user---206", "items": [{"id": 206, "name": "item---206", "price": 215.9}]})
 * Thread id: 57,watermater generate: 1558075721676
 * (3,641.7,1558075715000,1558075720000)
 * <p>
 * 模拟宕机，kill 程序，1min后拉起
 * <p>
 * 2019-05-17 14:49:52.192 [Kafka 0.10 Fetcher for Source: kafka-source -> Timestamps/Watermarks (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka version : 0.11.0.2
 * 2019-05-17 14:49:52.193 [Kafka 0.10 Fetcher for Source: kafka-source -> Timestamps/Watermarks (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka commitId : 73be1e1168f91ee2
 * 2019-05-17 14:49:52.207 [Kafka 0.10 Fetcher for Source: kafka-source -> Timestamps/Watermarks (1/1)] INFO  o.a.k.c.c.i.AbstractCoordinator - Discovered coordinator 192.168.5.109:9092 (id: 2147483646 rack: null) for group hqbhoho0001.
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 65, CreateTime = 1558075719676, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 205, value = {"id": 205, "name": "user---205", "items": [{"id": 205, "name": "item---205", "price": 214.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 66, CreateTime = 1558075733688, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 212, value = {"id": 212, "name": "user---212", "items": [{"id": 212, "name": "item---212", "price": 221.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 67, CreateTime = 1558075735689, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 213, value = {"id": 213, "name": "user---213", "items": [{"id": 213, "name": "item---213", "price": 222.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 68, CreateTime = 1558075737691, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 214, value = {"id": 214, "name": "user---214", "items": [{"id": 214, "name": "item---214", "price": 223.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 2, offset = 69, CreateTime = 1558075743699, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 217, value = {"id": 217, "name": "user---217", "items": [{"id": 217, "name": "item---217", "price": 226.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 28, CreateTime = 1558075727680, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 209, value = {"id": 209, "name": "user---209", "items": [{"id": 209, "name": "item---209", "price": 218.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 29, CreateTime = 1558075731686, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 211, value = {"id": 211, "name": "user---211", "items": [{"id": 211, "name": "item---211", "price": 220.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 30, CreateTime = 1558075741697, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 216, value = {"id": 216, "name": "user---216", "items": [{"id": 216, "name": "item---216", "price": 225.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 31, CreateTime = 1558075745708, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 218, value = {"id": 218, "name": "user---218", "items": [{"id": 218, "name": "item---218", "price": 227.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 1, offset = 32, CreateTime = 1558075749712, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 220, value = {"id": 220, "name": "user---220", "items": [{"id": 220, "name": "item---220", "price": 229.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 49, CreateTime = 1558075721676, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 206, value = {"id": 206, "name": "user---206", "items": [{"id": 206, "name": "item---206", "price": 215.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 50, CreateTime = 1558075723678, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 207, value = {"id": 207, "name": "user---207", "items": [{"id": 207, "name": "item---207", "price": 216.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 51, CreateTime = 1558075725678, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 208, value = {"id": 208, "name": "user---208", "items": [{"id": 208, "name": "item---208", "price": 217.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 52, CreateTime = 1558075729681, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 210, value = {"id": 210, "name": "user---210", "items": [{"id": 210, "name": "item---210", "price": 219.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 53, CreateTime = 1558075739694, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 215, value = {"id": 215, "name": "user---215", "items": [{"id": 215, "name": "item---215", "price": 224.9}]})
 * Thread id: 55,process ConsumerRecord(topic = flink-test-1, partition = 0, offset = 54, CreateTime = 1558075747710, serialized key size = 3, serialized value size = 425, headers = RecordHeaders(headers = [], isReadOnly = false), key = 219, value = {"id": 219, "name": "user---219", "items": [{"id": 219, "name": "item---219", "price": 228.9}]})
 * Thread id: 57,watermater generate: 1558075749712
 * (1,214.9,1558075715000,1558075720000)
 * (2,432.8,1558075720000,1558075725000)
 * (3,656.7,1558075725000,1558075730000)
 * (2,442.8,1558075730000,1558075735000)
 * (3,671.7,1558075735000,1558075740000)
 * (2,452.8,1558075740000,1558075745000)
 * Thread id: 57,watermater generate: 1558075749712
 * <p>
 * 可以看到  Triggering checkpoint 5 之后的205，206 重新消费了
 * 但是Triggering checkpoint 5 发生在 window[1558075715000,1558075720000]
 * 现在结果是：
 * 宕机前：(3,641.7,1558075715000,1558075720000)
 * 宕机后：(1,214.9,1558075715000,1558075720000)
 * <p>
 * 值得思考一下？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/15
 */
public class KafkaSourceExample {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2000);
        // 配置持久化statebackend
        StateBackend backend = new FsStateBackend(
                "file:/E:/flink_test/checkpoint",
                false);
        // 开启checkpoint
        env.enableCheckpointing(5000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // kafka consumer
        FlinkKafkaConsumer011<ConsumerRecord<String, User>> consumer = new FlinkKafkaConsumer011<ConsumerRecord<String, User>>(
                "flink-test-1",
                new KafkaAvroKeyedMessageDeserializationSchema(),
                loadProp()
        );
        // 消费模式
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromGroupOffsets();
        //  watermark will be generated partition num
//        consumer.assignTimestampsAndWatermarks(new KafkaMessageAssignTimestampAndWatermarks());
        // kafka source
        env.addSource(consumer, "kafka-source")
                .assignTimestampsAndWatermarks(new KafkaMessageAssignTimestampAndWatermarks())
                // 统计5s window 成交额
                .timeWindowAll(Time.seconds(5))
                .aggregate(new CountAggregateFuncation(), new CountProcessWindowFuncation())
                .print();

        env.execute("KafkaSourceExample");

    }

    /**
     * load kafka consumer config
     *
     * @return
     */

    private static Properties loadProp() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092");
        props.put("group.id", "hqbhoho0001");
        props.put("enable.auto.commit", true);
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    /**
     * kafka message generate watermark
     */
    static class KafkaMessageAssignTimestampAndWatermarks implements AssignerWithPeriodicWatermarks<ConsumerRecord<String, User>> {

        private Long currentMaxTimeStamp = 0L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            Optional.ofNullable("Thread id: " + Thread.currentThread().getId() + ",watermater generate: " + currentMaxTimeStamp)
                    .ifPresent(System.out::println);
            return new Watermark(currentMaxTimeStamp);
        }

        /**
         * @param element
         * @param previousElementTimestamp kafka message timestamp
         * @return
         */
        @Override
        public long extractTimestamp(ConsumerRecord<String, User> element, long previousElementTimestamp) {
//            Optional.ofNullable("element timestamp: "+element.timestamp()+",previousElementTimestamp: "+previousElementTimestamp)
//                    .ifPresent(System.out::println);
            currentMaxTimeStamp = Math.max(currentMaxTimeStamp, previousElementTimestamp);
            return previousElementTimestamp;
        }
    }

    /**
     * compute trade count and price sum
     */
    private static class CountAggregateFuncation implements AggregateFunction<ConsumerRecord<String, User>, Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> createAccumulator() {
            return Tuple2.of(0, 0.0);
        }

        @Override
        public Tuple2<Integer, Double> add(ConsumerRecord<String, User> value, Tuple2<Integer, Double> accumulator) {
            Optional.ofNullable("Thread id: " + Thread.currentThread().getId() + ",process " + value).ifPresent(System.out::println);
            return Tuple2.of(accumulator.f0 + 1, value.value().getItems().get(0).getPrice() + accumulator.f1);
        }

        @Override
        public Tuple2<Integer, Double> getResult(Tuple2<Integer, Double> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1);
        }

        @Override
        public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }


    /**
     * get window metadata to output message
     * <p>
     * (count,sum) ---> (count,sum,window.start_time,window.end_time)
     */
    private static class CountProcessWindowFuncation implements AllWindowFunction<Tuple2<Integer, Double>, Tuple4<Integer, Double, Long, Long>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<Integer, Double>> values, Collector<Tuple4<Integer, Double, Long, Long>> out) throws Exception {
            Tuple2<Integer, Double> next = values.iterator().next();
            out.collect(Tuple4.of(next.f0, next.f1, window.getStart(), window.getEnd()));
        }
    }
}
