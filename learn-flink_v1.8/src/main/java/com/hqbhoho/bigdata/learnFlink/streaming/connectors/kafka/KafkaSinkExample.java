package com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka;

import com.hqbhoho.bigdata.learnFlink.streaming.connectors.customer.MyDataSource;
import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.User;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.common.utils.Utils;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE;

/**
 * describe:
 * Kafka Sink Example
 * EXACTLY_ONCE 语义
 * <p>
 * <p>
 * 2019-05-17 17:06:42.317 [flink-akka.actor.default-dispatcher-7] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 3 for job f417ec6b065433403415aef797e9c27f (1973 bytes in 218 ms).
 * 2019-05-17 17:06:42.317 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.a.f.s.TwoPhaseCommitSinkFunction - FlinkKafkaProducer011 0/1 - checkpoint 3 complete, committing transaction TransactionHolder{handle=KafkaTransactionState [transactionalId=Source: source -> Sink: Unnamed-7df19f87deec5680128845fd9a6ca18d-2, producerId=55005, epoch=40], transactionStartTime=1558083997094} from checkpoint 3
 * 2019-05-17 17:06:42.321 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
 * <=======================> num: 5
 * <=======================> num: 6
 * 2019-05-17 17:06:47.318 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 4 @ 1558084007318 for job f417ec6b065433403415aef797e9c27f.
 * 2019-05-17 17:06:47.318 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.c.k.i.FlinkKafkaProducer - Flushing new partitions
 * 2019-05-17 17:06:47.320 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.c.producer.ProducerConfig - ProducerConfig values:
 * 2019-05-17 17:06:47.320 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Instantiated a transactional producer.
 * 2019-05-17 17:06:47.320 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Overriding the default retries config to the recommended value of 2147483647 since the idempotent producer is enabled.
 * 2019-05-17 17:06:47.320 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Overriding the default max.in.flight.requests.per.connection to 1 since idempontence is enabled.
 * 2019-05-17 17:06:47.320 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Overriding the default acks to all since idempotence is enabled.
 * 2019-05-17 17:06:47.324 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka version : 0.11.0.2
 * 2019-05-17 17:06:47.325 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka commitId : 73be1e1168f91ee2
 * 2019-05-17 17:06:47.325 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.c.k.FlinkKafkaProducer011 - Starting FlinkKafkaProducer (1/1) to produce into default topic flink-test-2
 * 2019-05-17 17:06:47.325 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.c.p.i.TransactionManager - [TransactionalId Source: source -> Sink: Unnamed-7df19f87deec5680128845fd9a6ca18d-0] ProducerId set to -1 with epoch -1
 * 2019-05-17 17:06:47.433 [kafka-producer-network-thread | producer-30] INFO  o.a.k.c.p.i.TransactionManager - [TransactionalId Source: source -> Sink: Unnamed-7df19f87deec5680128845fd9a6ca18d-0] ProducerId set to 54007 with epoch 38
 * 2019-05-17 17:06:47.434 [flink-akka.actor.default-dispatcher-8] INFO  o.a.f.r.c.CheckpointCoordinator - Completed checkpoint 4 for job f417ec6b065433403415aef797e9c27f (1973 bytes in 116 ms).
 * 2019-05-17 17:06:47.435 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.a.f.s.TwoPhaseCommitSinkFunction - FlinkKafkaProducer011 0/1 - checkpoint 4 complete, committing transaction TransactionHolder{handle=KafkaTransactionState [transactionalId=Source: source -> Sink: Unnamed-7df19f87deec5680128845fd9a6ca18d-1, producerId=56004, epoch=37], transactionStartTime=1558084002316} from checkpoint 4
 * 2019-05-17 17:06:47.441 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
 * <=======================> num: 7
 * <=======================> num: 8
 * <=======================> num: 9
 * 2019-05-17 17:06:51.700 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.c.k.i.FlinkKafkaProducer - Flushing new partitions
 * 2019-05-17 17:06:51.706 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
 * 2019-05-17 17:06:51.720 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.flink.runtime.taskmanager.Task - Source: source -> Sink: Unnamed (1/1) (7df05996401be404968c807bcd84532f) switched from RUNNING to FAILED.
 * java.lang.Exception: <==================================================================>
 * at com.hqbhoho.bigdata.learnFlink.streaming.connectors.customer.MyDataSource.run(MyDataSource.java:57) ~[classes/:na]
 * at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:93) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:57) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.runtime.tasks.SourceStreamTask.run(SourceStreamTask.java:97) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:300) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.runtime.taskmanager.Task.run(Task.java:711) ~[flink-runtime_2.11-1.8.0.jar:1.8.0]
 * at java.lang.Thread.run(Thread.java:748) [na:1.8.0_161]
 * 2019-05-17 17:06:51.720 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.flink.runtime.taskmanager.Task - Freeing task resources for Source: source -> Sink: Unnamed (1/1) (7df05996401be404968c807bcd84532f).
 * 2019-05-17 17:06:51.742 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.flink.runtime.taskmanager.Task - Ensuring all FileSystem streams are closed for task Source: source -> Sink: Unnamed (1/1) (7df05996401be404968c807bcd84532f) [FAILED]
 * 2019-05-17 17:06:51.747 [flink-akka.actor.default-dispatcher-6] INFO  o.a.f.r.taskexecutor.TaskExecutor - Un-registering task and sending final execution state FAILED to JobManager for task Source: source -> Sink: Unnamed 7df05996401be404968c807bcd84532f.
 * 2019-05-17 17:06:51.749 [flink-akka.actor.default-dispatcher-6] INFO  o.a.f.r.e.ExecutionGraph - Source: source -> Sink: Unnamed (1/1) (7df05996401be404968c807bcd84532f) switched from RUNNING to FAILED.
 * java.lang.Exception: <==================================================================>
 * at com.hqbhoho.bigdata.learnFlink.streaming.connectors.customer.MyDataSource.run(MyDataSource.java:57) ~[classes/:na]
 * at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:93) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:57) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.runtime.tasks.SourceStreamTask.run(SourceStreamTask.java:97) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:300) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.runtime.taskmanager.Task.run(Task.java:711) ~[flink-runtime_2.11-1.8.0.jar:1.8.0]
 * at java.lang.Thread.run(Thread.java:748) ~[na:1.8.0_161]
 * 2019-05-17 17:06:51.750 [flink-akka.actor.default-dispatcher-6] INFO  o.a.f.r.e.ExecutionGraph - Job KafkaSinkExample (f417ec6b065433403415aef797e9c27f) switched from state RUNNING to FAILING.
 * java.lang.Exception: <==================================================================>
 * at com.hqbhoho.bigdata.learnFlink.streaming.connectors.customer.MyDataSource.run(MyDataSource.java:57) ~[classes/:na]
 * at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:93) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:57) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.runtime.tasks.SourceStreamTask.run(SourceStreamTask.java:97) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:300) ~[flink-streaming-java_2.11-1.8.0.jar:1.8.0]
 * at org.apache.flink.runtime.taskmanager.Task.run(Task.java:711) ~[flink-runtime_2.11-1.8.0.jar:1.8.0]
 * at java.lang.Thread.run(Thread.java:748) ~[na:1.8.0_161]
 * <p>
 * restart job
 * <p>
 * 2019-05-17 17:06:51.752 [flink-akka.actor.default-dispatcher-2] INFO  o.a.f.r.taskexecutor.TaskExecutor - Discarding the results produced by task execution 7df05996401be404968c807bcd84532f.
 * 2019-05-17 17:06:51.755 [flink-akka.actor.default-dispatcher-6] INFO  o.a.f.r.e.ExecutionGraph - Try to restart or fail the job KafkaSinkExample (f417ec6b065433403415aef797e9c27f) if no longer possible.
 * 2019-05-17 17:06:51.755 [flink-akka.actor.default-dispatcher-6] INFO  o.a.f.r.e.ExecutionGraph - Job KafkaSinkExample (f417ec6b065433403415aef797e9c27f) switched from state FAILING to RESTARTING.
 * 2019-05-17 17:06:51.755 [flink-akka.actor.default-dispatcher-6] INFO  o.a.f.r.e.ExecutionGraph - Restarting the job KafkaSinkExample (f417ec6b065433403415aef797e9c27f).
 * .* ..............
 * 2019-05-17 17:07:52.842 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Instantiated a transactional producer.
 * 2019-05-17 17:07:52.842 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Overriding the default retries config to the recommended value of 2147483647 since the idempotent producer is enabled.
 * 2019-05-17 17:07:52.842 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Overriding the default max.in.flight.requests.per.connection to 1 since idempontence is enabled.
 * 2019-05-17 17:07:52.842 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.clients.producer.KafkaProducer - Overriding the default acks to all since idempotence is enabled.
 * 2019-05-17 17:07:52.844 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka version : 0.11.0.2
 * 2019-05-17 17:07:52.844 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.kafka.common.utils.AppInfoParser - Kafka commitId : 73be1e1168f91ee2
 * 2019-05-17 17:07:52.844 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.c.k.FlinkKafkaProducer011 - Starting FlinkKafkaProducer (1/1) to produce into default topic flink-test-2
 * 2019-05-17 17:07:52.845 [Source: source -> Sink: Unnamed (1/1)] INFO  o.a.k.c.p.i.TransactionManager - [TransactionalId Source: source -> Sink: Unnamed-7df19f87deec5680128845fd9a6ca18d-4] ProducerId set to -1 with epoch -1
 * 2019-05-17 17:07:52.956 [kafka-producer-network-thread | producer-38] INFO  o.a.k.c.p.i.TransactionManager - [TransactionalId Source: source -> Sink: Unnamed-7df19f87deec5680128845fd9a6ca18d-4] ProducerId set to 56003 with epoch 42
 * <=======================> num: 18
 * 2019-05-17 17:07:56.779 [Checkpoint Timer] INFO  o.a.f.r.c.CheckpointCoordinator - Triggering checkpoint 5 @ 1558084076779 for job f417ec6b065433403415aef797e9c27f.
 * 2019-05-17 17:07:56.782 [Async calls on Source: source -> Sink: Unnamed (1/1)] INFO  o.a.f.s.c.k.i.FlinkKafkaProducer - Flushing new partitions
 * <p>
 * chenkpoint   state   num = 7
 * ********************************************************************************************************************************************************************************************************
 * consumer1 :  props.put("isolation.level","read_committed");
 * <p>
 * 2019-05-17 17:05:13.038 [main] INFO  o.a.k.c.c.i.AbstractCoordinator - (Re-)joining group hqbhoho003
 * 2019-05-17 17:05:16.762 [main] INFO  o.a.k.c.c.i.AbstractCoordinator - Successfully joined group hqbhoho003 with generation 4
 * 2019-05-17 17:05:16.763 [main] INFO  o.a.k.c.c.i.ConsumerCoordinator - Setting newly assigned partitions [flink-test-2-2, flink-test-2-1, flink-test-2-0] for group hqbhoho003
 * 2019-05-17 17:06:32.218 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:0,partition:2,value:{"id": 0, "name": "user---0", "items": [{"id": 0, "name": "item---0", "price": 9.9}]},
 * 2019-05-17 17:06:37.104 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:2,partition:2,value:{"id": 2, "name": "user---2", "items": [{"id": 2, "name": "item---2", "price": 11.9}]},
 * 2019-05-17 17:06:37.106 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:1,partition:0,value:{"id": 1, "name": "user---1", "items": [{"id": 1, "name": "item---1", "price": 10.9}]},
 * 2019-05-17 17:06:42.328 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:3,partition:2,value:{"id": 3, "name": "user---3", "items": [{"id": 3, "name": "item---3", "price": 12.9}]},
 * 2019-05-17 17:06:42.330 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:4,partition:1,value:{"id": 4, "name": "user---4", "items": [{"id": 4, "name": "item---4", "price": 13.9}]},
 * 2019-05-17 17:06:42.330 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:5,partition:0,value:{"id": 5, "name": "user---5", "items": [{"id": 5, "name": "item---5", "price": 14.9}]},
 * 2019-05-17 17:06:47.445 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:6,partition:1,value:{"id": 6, "name": "user---6", "items": [{"id": 6, "name": "item---6", "price": 15.9}]},
 * 2019-05-17 17:06:47.445 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:7,partition:0,value:{"id": 7, "name": "user---7", "items": [{"id": 7, "name": "item---7", "price": 16.9}]},
 * 2019-05-17 17:07:57.036 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:18,partition:1,value:{"id": 18, "name": "user---18", "items": [{"id": 18, "name": "item---18", "price": 27.9}]},
 * 2019-05-17 17:07:57.037 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:19,partition:1,value:{"id": 19, "name": "user---19", "items": [{"id": 19, "name": "item---19", "price": 28.9}]},
 * 2019-05-17 17:08:02.151 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:21,partition:0,value:{"id": 21, "name": "user---21", "items": [{"id": 21, "name": "item---21", "price": 30.9}]},
 * 2019-05-17 17:08:02.152 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:22,partition:0,value:{"id": 22, "name": "user---22", "items": [{"id": 22, "name": "item---22", "price": 31.9}]},
 * 2019-05-17 17:08:02.154 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:20,partition:1,value:{"id": 20, "name": "user---20", "items": [{"id": 20, "name": "item---20", "price": 29.9}]},
 * ***************************************************************************************************************************************************************************************************************
 * consumer1 :  props.put("isolation.level","read_uncommitted");
 * <p>
 * *019-05-17 17:06:22.529 [main] INFO  o.a.k.c.c.i.AbstractCoordinator - (Re-)joining group hqbhoho004
 * 2019-05-17 17:06:22.537 [main] INFO  o.a.k.c.c.i.AbstractCoordinator - Successfully joined group hqbhoho004 with generation 7
 * 2019-05-17 17:06:22.537 [main] INFO  o.a.k.c.c.i.ConsumerCoordinator - Setting newly assigned partitions [flink-test-2-2, flink-test-2-1, flink-test-2-0] for group hqbhoho004
 * 2019-05-17 17:06:31.969 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:0,partition:2,value:{"id": 0, "name": "user---0", "items": [{"id": 0, "name": "item---0", "price": 9.9}]},
 * 2019-05-17 17:06:33.683 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:1,partition:0,value:{"id": 1, "name": "user---1", "items": [{"id": 1, "name": "item---1", "price": 10.9}]},
 * 2019-05-17 17:06:35.684 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:2,partition:2,value:{"id": 2, "name": "user---2", "items": [{"id": 2, "name": "item---2", "price": 11.9}]},
 * 2019-05-17 17:06:37.691 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:3,partition:2,value:{"id": 3, "name": "user---3", "items": [{"id": 3, "name": "item---3", "price": 12.9}]},
 * 2019-05-17 17:06:39.697 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:4,partition:1,value:{"id": 4, "name": "user---4", "items": [{"id": 4, "name": "item---4", "price": 13.9}]},
 * 2019-05-17 17:06:41.702 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:5,partition:0,value:{"id": 5, "name": "user---5", "items": [{"id": 5, "name": "item---5", "price": 14.9}]},
 * 2019-05-17 17:06:43.696 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:6,partition:1,value:{"id": 6, "name": "user---6", "items": [{"id": 6, "name": "item---6", "price": 15.9}]},
 * 2019-05-17 17:06:45.701 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:7,partition:0,value:{"id": 7, "name": "user---7", "items": [{"id": 7, "name": "item---7", "price": 16.9}]},
 * 2019-05-17 17:06:47.703 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:8,partition:0,value:{"id": 8, "name": "user---8", "items": [{"id": 8, "name": "item---8", "price": 17.9}]},
 * 2019-05-17 17:06:49.702 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:9,partition:2,value:{"id": 9, "name": "user---9", "items": [{"id": 9, "name": "item---9", "price": 18.9}]},
 * 2019-05-17 17:07:52.973 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:18,partition:1,value:{"id": 18, "name": "user---18", "items": [{"id": 18, "name": "item---18", "price": 27.9}]},
 * 2019-05-17 17:07:54.964 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:19,partition:1,value:{"id": 19, "name": "user---19", "items": [{"id": 19, "name": "item---19", "price": 28.9}]},
 * 2019-05-17 17:07:57.030 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:20,partition:1,value:{"id": 20, "name": "user---20", "items": [{"id": 20, "name": "item---20", "price": 29.9}]},
 * 2019-05-17 17:07:59.023 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:21,partition:0,value:{"id": 21, "name": "user---21", "items": [{"id": 21, "name": "item---21", "price": 30.9}]},
 * 2019-05-17 17:08:01.024 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:22,partition:0,value:{"id": 22, "name": "user---22", "items": [{"id": 22, "name": "item---22", "price": 31.9}]},
 * 2019-05-17 17:08:03.028 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:23,partition:0,value:{"id": 23, "name": "user---23", "items": [{"id": 23, "name": "item---23", "price": 32.9}]},
 * 2019-05-17 17:08:05.031 [main] INFO  c.h.b.l.consumer.MyNewConsumer - <====> key:24,partition:1,value:{"id": 24, "name": "user---24", "items": [{"id": 24, "name": "item---24", "price": 33.9}]},
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/17
 */
public class KafkaSinkExample {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 配置持久化statebackend
        StateBackend backend = new FsStateBackend(
                "file:/E:/flink_test/checkpoint",
                false);
        // 开启重试策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(60, TimeUnit.SECONDS) // delay
        ));
        // 开启checkpoint
        env.enableCheckpointing(2000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(5000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.setFailOnCheckpointingErrors(true);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // Kafka Sink
        FlinkKafkaProducer011<User> producer = new FlinkKafkaProducer011<>(
                "flink-test-2",
                new KafkaAvroKeyedMessageSerializationSchema(),
                initProps(),
                Optional.of(new KeyPartitioner()),
                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE,
                DEFAULT_KAFKA_PRODUCERS_POOL_SIZE
        );
        producer.setWriteTimestampToKafka(true);
        env.addSource(new MyDataSource(), "source")
                .addSink(producer);

        env.execute("KafkaSinkExample");

    }

    /**
     * load kafka producer config
     *
     * @return
     */
    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.108:9092");
        // 同broker配置，不然会报错
        props.put("transaction.timeout.ms", 15 * 60 * 1000);
        return props;
    }


    /**
     * 自定义Partitioner
     * hashcode(key) % partitons.num
     * 默认分区规则是:  1个subtask ---> 1个partition
     */
    private static class KeyPartitioner extends FlinkKafkaPartitioner<User> {
        @Override
        public int partition(User record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            return Utils.toPositive(Utils.murmur2(key)) % partitions.length;
        }
    }
}
