package com.hqbhoho.bigdata.learnFlink.streaming.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * describe:
 * vcores: 4
 * https://flink.apache.org/visualizer/
 * {"nodes":[{"id":1,"type":"Source: Socket Stream","pact":"Data Source","contents":"Source: Socket Stream","parallelism":1},{"id":2,"type":"Map","pact":"Operator","contents":"Map","parallelism":4,"predecessors":[{"id":1,"ship_strategy":"REBALANCE","side":"second"}]},{"id":3,"type":"Filter","pact":"Operator","contents":"Filter","parallelism":4,"predecessors":[{"id":2,"ship_strategy":"FORWARD","side":"second"}]},{"id":4,"type":"Sink: Print to Std. Out","pact":"Data Sink","contents":"Sink: Print to Std. Out","parallelism":4,"predecessors":[{"id":3,"ship_strategy":"FORWARD","side":"second"}]}]}
 *
 * operator chain :将并行度相同的operator合并到一个task中去
 * start log
 * *****************************************************************************************************************************************
 * 1. default:
 *
 * Source
 * Map -> Filter -> Map -> Sink
 *
 * [Source: Socket Stream (1/1)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Source: Socket Stream (1/1) (eb12f37a36668c87d9b7fc346e1354c7) [DEPLOYING].
 * [Map -> Filter -> Map -> Sink: Print to Std. Out (1/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map -> Filter -> Map -> Sink: Print to Std. Out (1/4) (c4cc0174def4593bee095c96a853ca12) [DEPLOYING].
 * [Map -> Filter -> Map -> Sink: Print to Std. Out (2/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map -> Filter -> Map -> Sink: Print to Std. Out (2/4) (13eb69cf94989c58e43fc914c88cb96b) [DEPLOYING].
 * [Map -> Filter -> Map -> Sink: Print to Std. Out (3/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map -> Filter -> Map -> Sink: Print to Std. Out (3/4) (adeeda84313d0414fe9ed65471aa46a4) [DEPLOYING].
 * [Map -> Filter -> Map -> Sink: Print to Std. Out (4/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map -> Filter -> Map -> Sink: Print to Std. Out (4/4) (fff433a51da40fc3e54bb6a434895fd7) [DEPLOYING].
 * ****************************************************************************************************************************************
 * 2. 全局禁用   env.disableOperatorChaining():
 *
 * Source
 * Map
 * Filter
 * Map
 * Sink
 *
 * [Source: Socket Stream (1/1)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Source: Socket Stream (1/1) (6a01ed04a67421d6a2ed553475384dfd) [DEPLOYING].
 * [Map (1/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map (1/4) (3fd29b911551497ede86f51f883ab90e) [DEPLOYING].
 * [Map (2/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map (2/4) (52677f0ef26a691b610b287a72614faa) [DEPLOYING].
 * [Map (3/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map (3/4) (11180db08a498fec68834a400880ba5a) [DEPLOYING].
 * [Map (4/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Map (4/4) (21670788f47a1b42211cebf2c7836fd7) [DEPLOYING].
 * [Filter (1/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Filter (1/4) (51444f10f814e502c4a0a2187bb9c790) [DEPLOYING].
 * [Filter (2/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Filter (2/4) (818151a3cb33c2268045e0b6ba29dc27) [DEPLOYING].
 * [Filter (3/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Filter (3/4) (c63a000244e6513c0975e7df5e093df4) [DEPLOYING].
 * [Filter (4/4)] INFO  o.a.flink.runtime.taskmanager.Task - Loading JAR files for task Filter (4/4) (313456cfb28218e58a4fc01f97ca7580) [DEPLOYING].
 * 剩下的map 与 sink operator  也同上，分别各自起了4个task
 *****************************************************************************************************************************************
 * 3. 单个算子， filter().disableChaining()
 *
 *  Source
 *  Map
 *  Filter
 *  Map -> Sink
 *
 *  Source: Socket Stream (1/1) (37d2bcc7a64acbfff78eea2f9974a1e0) switched from SCHEDULED to DEPLOYING.
 *  Deploying Source: Socket Stream (1/1) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map (1/4) (2a1e6feb09c567beb9a5107efbcb4e81) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map (1/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map (2/4) (fb9c1e413dff04bbe4e2a7810379c7d0) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map (2/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map (3/4) (c5accce479d7fafb8bd1b758ec90605c) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map (3/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map (4/4) (c858bfcf5e1741980989fbb11c6d71e8) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map (4/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Filter (1/4) (d23af262d8efbd4003fa5036187b8fea) switched from SCHEDULED to DEPLOYING.
 *  Deploying Filter (1/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Filter (2/4) (d0331fe31e70deb480432d1942938a0d) switched from SCHEDULED to DEPLOYING.
 *  Deploying Filter (2/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Filter (3/4) (a1205bf809fc914b3434338eddac3eaf) switched from SCHEDULED to DEPLOYING.
 *  Deploying Filter (3/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Filter (4/4) (d0263b2ce61c796cd03eca51c8c23d64) switched from SCHEDULED to DEPLOYING.
 *  Deploying Filter (4/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map -> Sink: Print to Std. Out (1/4) (f6425151e9e1d5fc5136cb8b98ba3bc0) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map -> Sink: Print to Std. Out (1/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map -> Sink: Print to Std. Out (2/4) (e63e8b7329508317556fca1f71213971) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map -> Sink: Print to Std. Out (2/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map -> Sink: Print to Std. Out (3/4) (d15644ad394f6a70c51eae602ccd6ddb) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map -> Sink: Print to Std. Out (3/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 *  Map -> Sink: Print to Std. Out (4/4) (9a199c9f2fb0c05dab423a35b8794c51) switched from SCHEDULED to DEPLOYING.
 *  Deploying Map -> Sink: Print to Std. Out (4/4) (attempt #0) to 2ee34b6b-d2c0-4d47-9e4c-e8343e875e61 @ 127.0.0.1 (dataPort=-1)
 ******************************************************************************************************************************************
 * 4. 单个算子, filter().startNewChain()
 *
 * Source
 * Map
 * Filter -> Map -> Sink
 *
 * Source: Socket Stream (1/1) (262f7a13c464f2c0d316ac44b6bb4192) switched from SCHEDULED to DEPLOYING.
 * Deploying Source: Socket Stream (1/1) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Map (1/4) (13835cdac57c46b263ade4d089d4686b) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (1/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Map (2/4) (e70ac424911f1b4f89f2711163d161d0) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (2/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Map (3/4) (d4e710c6db1fffb92e596b4ceaf23802) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (3/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Map (4/4) (9cf02bce7a5839364c07d32103d51202) switched from SCHEDULED to DEPLOYING.
 * Deploying Map (4/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Filter -> Map -> Sink: Print to Std. Out (1/4) (1c31a1d1cee4235babebbc9e8a8f8236) switched from SCHEDULED to DEPLOYING.
 * Deploying Filter -> Map -> Sink: Print to Std. Out (1/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Filter -> Map -> Sink: Print to Std. Out (2/4) (5ba1d8f6a3d2e3b0d76263fbf56b8819) switched from SCHEDULED to DEPLOYING.
 * Deploying Filter -> Map -> Sink: Print to Std. Out (2/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Filter -> Map -> Sink: Print to Std. Out (3/4) (795da20aab8a74b230d0eed0f6cb3764) switched from SCHEDULED to DEPLOYING.
 * Deploying Filter -> Map -> Sink: Print to Std. Out (3/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 * Filter -> Map -> Sink: Print to Std. Out (4/4) (4b70d935870ba2364d51bba221960852) switched from SCHEDULED to DEPLOYING.
 * Deploying Filter -> Map -> Sink: Print to Std. Out (4/4) (attempt #0) to 7d154e78-9032-4ac8-a497-ff5ed0fb98c9 @ 127.0.0.1 (dataPort=-1)
 ******************************************************************************************************************************************
 * 资源共享，slot group
 * 5. 单个算子，filter.slotSharingGroup("fliter")  slotSharingGroup不指定，默认都是defualt
 *
 * env.setParallelism(1);
 * source->map
 * fliter->map->Sink
 *
 * task slot num < 2  ,job nou run!
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/13
 */
public class OperatorChainExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.disableOperatorChaining();
        env.setParallelism(1);
        env.socketTextStream("10.105.1.182",19999)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value;
                    }
                })
                .filter(v -> v == null)
//                .disableChaining()
//                .startNewChain()
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value;
                    }
                })
                .print();
        env.execute("OperatorChainExample");
    }
}
