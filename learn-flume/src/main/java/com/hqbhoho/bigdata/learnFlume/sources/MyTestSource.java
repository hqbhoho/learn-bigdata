package com.hqbhoho.bigdata.learnFlume.sources;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * describe:
 * 自定义Flume Source,每20s读取一次文件,为每一行event加入特定的headers,关闭source时，修改文件的后缀为.COMPLETED
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/03
 */
public class MyTestSource extends AbstractSource implements Configurable,PollableSource{

    //开启日志
    private final static Logger LOG = LoggerFactory.getLogger(MyTestSource.class);
    //指定在配置文件中的key值
    private final String FILE_PATH="file.path";
    //读取的文件路径
    private String file_path;
    private Path file;
    //自定义headers
    Map<String, String> headers = new HashMap<>();

    @Override
    public void configure(Context context) {
        file_path=context.getString("file.path");
        file=Paths.get(file_path);
    }

    /**
     * 完成Source创建init工作。
     */
    @Override
    public synchronized void start() {
        super.start();
        try {
            //添加headers信息
            headers.put("type", "plainData");
            headers.put("path",file_path );
        } catch (Throwable e) {
            LOG.error("MySource start Failed.",e);
            throw new RuntimeException(e);
        }
        LOG.info("MySource start Success.");
    }

    /**
     * 完成Source停止工作,更改文件名，加入后缀.COMPLETED。
     */
    @Override
    public synchronized void stop() {
        super.stop();
        try {
            Files.copy(file, Paths.get(file_path + ".COMPLETED"));
            Files.delete(file);
        } catch (IOException e) {
            LOG.error("MySource stop Failed.",e);
            throw new RuntimeException(e);
        }
        LOG.info("MySource stop Success.");
    }

    /**
     * 每20s读取一次文件,读取数据，为event添加自定义headers
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        Event event = new SimpleEvent();
        StringBuilder builder = new StringBuilder();
        String line;
        try(BufferedReader reader =  new BufferedReader(new InputStreamReader(Files.newInputStream(file)))) {
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            //组装event
            headers.put("timeStamp", String.valueOf(System.currentTimeMillis()));
            event.setHeaders(headers);
            event.setBody(builder.toString().getBytes());
            //发送event
            getChannelProcessor().processEvent(event);
            //休眠20s
            TimeUnit.SECONDS.sleep(20);
        }catch (Throwable e){
            LOG.error("Error:", e);
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 1000;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 1000;
    }
}
