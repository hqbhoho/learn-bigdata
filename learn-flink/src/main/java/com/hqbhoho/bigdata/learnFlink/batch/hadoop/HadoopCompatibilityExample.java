package com.hqbhoho.bigdata.learnFlink.batch.hadoop;

/**
 * describe:
 * <p>
 * Hadoop Compatibility
 * 注意在测试的时候    avro  版本有冲突
 * 运行此处代码是需要将flink-avro注释，flink stream测试代码中connectors下的代码全注释掉。
 * <p>
 * Test：
 * input ---> E:\flink_test\hadoop\input\test.txt
 * hello flink
 * hello spark
 * good good study day day up
 * spark sql streaming kafka kafka
 * output ---> E:\flink_test\hadoop\output\
 * 默认并行度为4，output文件下会生成4个文件
 * 1
 * good	2
 * hello	2
 * spark	2
 * up	1
 * 2
 * day	2
 * flink	1
 * 3
 * kafka	2
 * sql	1
 * study	1
 * 4
 * streaming	1
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/05/24
 */
/*public class HadoopCompatibilityExample {
    public static void main(String[] args) throws Exception {
        //获取参数
        ParameterTool tool = ParameterTool.fromArgs(args);
        String inPath = tool.get("inPath", "E:\\flink_test\\hadoop\\input");
        String outPath = tool.get("outPath", "E:\\flink_test\\hadoop\\output");

        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Hadoop input format
        JobConf conf = new JobConf(new Configuration());
        HadoopInputFormat<LongWritable, Text> hadoopInput = HadoopInputs.createHadoopInput(
                new TextInputFormat(),
                LongWritable.class,
                Text.class,
                conf
        );

        // 创建输入
        DataSet<Tuple2<LongWritable, Text>> input =
                env.createInput(hadoopInput);
        TextInputFormat.addInputPath(conf, new Path(inPath));

        // mapper计算
        DataSet<Tuple2<Text, LongWritable>> result = input.flatMap(
                new HadoopMapFunction<LongWritable, Text, Text, IntWritable>(
                        new WCMapper()
                )
        )
                .groupBy(0)
                // reduce计算
                .reduceGroup(
                        new HadoopReduceCombineFunction(
                                new WCReduce(), new WCReduce()
                        )
                );
        // Hadoop output format
        HadoopOutputFormat<Text, LongWritable> hadoopOutput = new HadoopOutputFormat<>(
                new TextOutputFormat<Text, LongWritable>(),
                conf
        );
        TextOutputFormat.setOutputPath(conf, new Path(outPath));
        // Emit data using the Hadoop TextOutputFormat.
        result.output(hadoopOutput);
        // Execute Program
        env.execute("Hadoop WordCount");
    }

    // mapreduce 原生代码

    *//**
     * 编写map任务业务逻辑,主要重写map()方法
     *//*
    public static class WCMapper implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        // 定义map任务输出键值对
        private static Text mapOutputKey = new Text();
        private static final IntWritable mapOutputValue = new IntWritable(1);

        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String inputStr = text.toString();
            // 使用StringTokenizer类进行字符串切割迭代
            StringTokenizer st = new StringTokenizer(inputStr);
            while (st.hasMoreTokens()) {
                mapOutputKey.set(st.nextToken());
                outputCollector.collect(mapOutputKey, mapOutputValue);
            }
        }

        @Override
        public void configure(JobConf jobConf) {
            // do nothing
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
    }

    *//**
     * 编写reduce任务业务逻辑,主要重写reduce()方法
     *//*
    public static class WCReduce implements
            Reducer<Text, IntWritable, Text, IntWritable> {

        // 定义map任务输出键值对,输出key同输入key
        private static IntWritable outputValue = new IntWritable();

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            //统计求和计算
            while (values.hasNext()) {
                sum += values.next().get();

            }
            outputValue.set(sum);
            outputCollector.collect(key, outputValue);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }

        @Override
        public void configure(JobConf jobConf) {
            // do nothing
        }
    }
}*/
