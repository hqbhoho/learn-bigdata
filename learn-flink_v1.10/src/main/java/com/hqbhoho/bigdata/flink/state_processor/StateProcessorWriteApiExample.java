package com.hqbhoho.bigdata.flink.state_processor;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.StateBootstrapFunction;

import java.util.Arrays;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/27
 */
public class StateProcessorWriteApiExample {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> listState = bEnv.fromElements(99999);
        BootstrapTransformation transformation1 = OperatorTransformation
                .bootstrapWith(listState)
                .transform(new SimpleBootstrapFunction())
                ;

        int maxParallelism = 2;

        Savepoint
                .load(bEnv,"file:/E:/flink_test/savepoint/savepoint-736147-226c04897428",new MemoryStateBackend())

                .withOperator("operator_state", transformation1)

                .write("E:/flink_test/savepoint/savepoint-444444");

        bEnv.execute();

    }

    public static class SimpleBootstrapFunction extends StateBootstrapFunction<Integer> {

        private ListState<Integer> state;
        private int count;

        @Override
        public void processElement(Integer value, Context ctx) throws Exception {
            count = value;
            System.out.println(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("==============="+count);
            state.update(Arrays.asList(count));
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Integer> listStateDescriptor =
                    new ListStateDescriptor<Integer>("checkPointCountList", Types.INT);
            state = context.getOperatorStateStore().getListState(listStateDescriptor);
        }
    }


}
