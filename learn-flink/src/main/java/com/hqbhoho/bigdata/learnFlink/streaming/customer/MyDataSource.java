package com.hqbhoho.bigdata.learnFlink.streaming.customer;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * describe:
 * Customer Datasource
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/30
 */
public class MyDataSource implements SourceFunction<Tuple3<Long,String,Integer>> {
    @Override
    public void run(SourceContext<Tuple3<Long, String, Integer>> sourceContext) throws Exception {
        while(true){
//            sourceContext.collect(new Tuple3<Long, String, Integer>(System.currentTimeMillis(), "hqbhoho1", 300));
//            sourceContext.collect(new Tuple3<Long, String, Integer>(System.currentTimeMillis()-1000, "hqbhoho1", -100));
//            sourceContext.collect(new Tuple3<Long, String, Integer>(System.currentTimeMillis()-2000, "hqbhoho1", -200));
            sourceContext.collect(new Tuple3<Long, String, Integer>(System.currentTimeMillis(), "hqbhoho", 300));
            TimeUnit.SECONDS.sleep(3);
        }
    }

    /**
     * flink cancel task,this method will be invoked
     */
    @Override
    public void cancel() {
        // do nothing
    }
}
