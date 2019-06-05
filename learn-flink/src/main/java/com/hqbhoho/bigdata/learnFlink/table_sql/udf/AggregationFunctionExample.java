package com.hqbhoho.bigdata.learnFlink.table_sql.udf;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

/**
 * describe:
 * <p>
 * 自定义聚合函数  Aggregation Function
 * <p>
 * result:
 * 1> (true,iphone6,800.0)
 * 1> (true,iphone5,800.0)
 * 1> (false,iphone6,800.0)
 * 1> (true,iphone6,1120.0)
 * 1> (false,iphone5,800.0)
 * 1> (true,iphone5,640.0)
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/30
 */
public class AggregationFunctionExample {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置全局配置参数
        Configuration conf = new Configuration();
        conf.setDouble("weight_factor", 0.2);
        env.getConfig().setGlobalJobParameters(conf);

        // 注册函数
        tableEnv.registerFunction("weight_avg", new WeightedAvgFunction());

        DataStreamSource<Tuple3<String, Double, Integer>> input = env.fromElements(
                Tuple3.of("iphone5", 2000.00, 4),
                Tuple3.of("iphone6", 4000.00, 6),
                Tuple3.of("iphone5", 4000.00, 6),
                Tuple3.of("iphone6", 8000.00, 4)
        );

        // 注册表  DataStream  --->  Table
        tableEnv.registerDataStream("example", input, "item,price,num");

        // table api
        Table resultTable1 = tableEnv.scan("example")
                .groupBy("item")
                .select("item,weight_avg(price,num) as avg");
        // sql api
        Table resultTable2 = tableEnv.sqlQuery("select item,weight_avg(price,num) from example group by item");


        //  Table  ---> DataStream
        /*tableEnv.toRetractStream(resultTable1, Row.class)
                .print();*/
        tableEnv.toRetractStream(resultTable2, Row.class)
                .print();
        env.execute("AggregationFunctionExample");
    }

    /**
     * 自定义聚合函数
     */
    public static class WeightedAvgFunction extends AggregateFunction<Double, WeightedAvgAccum> {

        // 定义加权因子
        private double factor;

        @Override
        public void open(FunctionContext context) throws Exception {
            // Global job parameter value associated with given key.
            factor = Double.valueOf(context.getJobParameter("weight_factor", "1.0"));
        }

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        public void accumulate(WeightedAvgAccum accumulator, double price, int num) {
            accumulator.incrby(num * price * factor, num);
        }

        public void retract(WeightedAvgAccum accumulator, double price, int num) {
            accumulator.decrby(num * price * factor, num);
        }

        public void merge(WeightedAvgAccum accumulator, java.lang.Iterable<WeightedAvgAccum> iterable) {
            for (WeightedAvgAccum acc : iterable) {
                accumulator.incrby(acc.getSum(), acc.getCount());
            }
        }

        public void resetAccumulator(WeightedAvgAccum accumulator) {
            accumulator.reset();
        }

        public Double getValue(WeightedAvgAccum accumulator) {
            return accumulator.getSum() / accumulator.getCount();
        }

        public TypeInformation<Double> getResultType() {
            return BasicTypeInfo.DOUBLE_TYPE_INFO;
        }
    }

    /**
     * 自定义的accumulator类
     * 加权计算平均值
     */
    public static class WeightedAvgAccum {
        private double sum;
        private int count;

        public WeightedAvgAccum() {
            this.sum = 0;
            this.count = 0;
        }

        public WeightedAvgAccum(double sum, int count) {
            this.sum = sum;
            this.count = count;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        // reset
        public void reset() {
            this.sum = 0;
            this.count = 0;
        }

        // add
        public void incrby(double sum, int count) {
            this.sum += sum;
            this.count += count;
        }

        // remove
        public void decrby(double sum, int count) {
            this.sum -= sum;
            this.count -= count;
        }
    }
}
