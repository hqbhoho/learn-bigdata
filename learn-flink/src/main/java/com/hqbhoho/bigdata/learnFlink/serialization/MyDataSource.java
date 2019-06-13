package com.hqbhoho.bigdata.learnFlink.serialization;

import com.hqbhoho.bigdata.learnFlink.serialization.protobuf.ProtobufModel;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * describe:
 * Customer Datasource
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/04/30
 */


public class MyDataSource extends RichSourceFunction<ProtobufModel.Account>  {


    @Override
    public void run(SourceContext<ProtobufModel.Account> ctx) throws Exception {
        int num = 1;
        while (num <= 5) {
            ctx.collect(ProtobufModel.Account.newBuilder()
                    .setName("A")
                    .setAccount(num*100)
                    .setTimestamp(System.currentTimeMillis())
                    .build()
            );
            ctx.collect(ProtobufModel.Account.newBuilder()
                    .setName("B")
                    .setAccount(num*100)
                    .setTimestamp(System.currentTimeMillis())
                    .build()
            );
            ctx.collect(ProtobufModel.Account.newBuilder()
                    .setName("C")
                    .setAccount(num*100)
                    .setTimestamp(System.currentTimeMillis())
                    .build()
            );
            num++;
        }
    }

    @Override
    public void cancel() {
        // do nothing
    }

}
