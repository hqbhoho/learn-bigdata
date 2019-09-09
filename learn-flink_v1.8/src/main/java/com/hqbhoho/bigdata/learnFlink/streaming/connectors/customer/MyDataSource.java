package com.hqbhoho.bigdata.learnFlink.streaming.connectors.customer;

import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.Item;
import com.hqbhoho.bigdata.learnFlink.streaming.connectors.kafka.avro.User;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * Customer Datasource
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/30
 */


public class MyDataSource extends RichSourceFunction<User> implements CheckpointedFunction {

    private Integer num;

    private ListState<Integer> numState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        numState.update(Arrays.asList(num));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Integer> numStateDescriptor =
                new ListStateDescriptor<Integer>("numStateList", BasicTypeInfo.INT_TYPE_INFO);
        numState = context.getOperatorStateStore().getListState(numStateDescriptor);
        // 如果是失败恢复的，从状态中恢复数据
        if (context.isRestored()) {
            num = numState.get().iterator().next() + 11;
        }
    }

    @Override
    public void run(SourceContext<User> ctx) throws Exception {
        while (true) {
            if (num == null) {
                num = 0;
            }
            // 模拟故障，自动重启
            if (num == 10) {
                throw new Exception("<==================================================================>");
            }
            List<Item> items = new ArrayList<>();
            items.add(new Item(num, "item---" + num, num + 9.9));
            ctx.collect(new User(num, "user---" + num, items));
            TimeUnit.SECONDS.sleep(2);
            Optional.of("<=======================> num: " + num).ifPresent(System.out::println);
            num++;
        }
    }

    @Override
    public void cancel() {
        // do nothing
    }

}
