package com.hqbhoho.bigdata.flink.state_processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

/**
 * describe:
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2020/05/26
 */
public class StateProcessorReadApiExample {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "file:/E:/flink_test/savepoint/savepoint-736147-226c04897428", new MemoryStateBackend());
        DataSet<Integer> listState  = savepoint.readListState("operator_state","checkPointCountList",Types.INT);
        listState.print();
        DataSet<KeyedState> keyedState = savepoint.readKeyedState("keyed_state", new ReaderFunction());
        keyedState.print();




    }

    public static class KeyedState {
        public String key;

        public int value;

        @Override
        public String toString() {
            return "KeyedState{" +
                    "key=" + key +
                    ", value=" + value +
                    '}';
        }
    }

    public static class ReaderFunction extends KeyedStateReaderFunction<String, KeyedState> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> valueState =
                    new ValueStateDescriptor<Integer>("value", Types.INT, 0);
            countState = getRuntimeContext().getState(valueState);

        }

        @Override
        public void readKey(
                String key,
                Context ctx,
                Collector<KeyedState> out) throws Exception {

            KeyedState data = new KeyedState();
            data.key = key;
            data.value = countState.value();
            out.collect(data);
        }
    }

}
