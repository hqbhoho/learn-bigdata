package com.hqbhoho.bigdata.flink.stream;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


public class Source extends RichSourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return super.getIterationRuntimeContext();
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    /**
     * 可以在这里将数据输出
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext ctx) throws Exception {

        while (isRunning) {
            ctx.collect("hello,world,flink,spark");
            Thread.sleep(1000);
        }
    }

    /**
     *
     */
    @Override
    public void cancel() {

        isRunning = false;
    }


}
