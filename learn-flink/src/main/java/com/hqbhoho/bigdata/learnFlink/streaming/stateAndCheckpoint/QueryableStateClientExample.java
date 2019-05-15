package com.hqbhoho.bigdata.learnFlink.streaming.stateAndCheckpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * describe:
 * <p>
 * Queryable State Client Example
 * <p>
 * 测试：
 * ./bin/flink run -c com.hqbhoho.bigdata.learnFlink.streaming.StateAndCheckpoint.QueryableStateExample /opt/learn-flink-1.0-SNAPSHOT.jar --host 192.168.5.131 --port 29999
 * nc -l 29999
 * hqb,200
 * hqbhoho,400
 * hqbhoho,600
 * 结果：
 * key: hqbhoho,state: 2,1000
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/09
 */
public class QueryableStateClientExample {
    public static void main(String[] args) throws Exception {

        // 获取Job id
        JobID jobId = JobID.fromHexString("f59d9eff9d2b74490e0e63aa011026b0");
        // 创建查询客户端
        QueryableStateClient client = new QueryableStateClient("192.168.5.131", 9069);
        // 状态描述，同流处理中定义的一样
        ListStateDescriptor<Tuple2<Integer, Integer>> countStateDescriptor =
                new ListStateDescriptor<Tuple2<Integer, Integer>>(
                        "countStateDescriptor",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        })
                );
        // 获取状态
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<ListState<Tuple2<Integer, Integer>>> results =
                client.getKvState(jobId, "count-query", "hqbhoho", BasicTypeInfo.STRING_TYPE_INFO, countStateDescriptor);
        results.whenComplete((v, e) -> {
            if (e == null) {
                try {
                    Tuple2<Integer, Integer> next = v.get().iterator().next();
                    Optional.ofNullable("key: hqbhoho,state: " + next.f0 + "," + next.f1).ifPresent(System.out::println);
                    latch.countDown();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
            }
        });
        latch.await();
//        client.shutdownAndWait();
    }
}
