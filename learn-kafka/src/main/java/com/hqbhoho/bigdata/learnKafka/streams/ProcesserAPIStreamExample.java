package com.hqbhoho.bigdata.learnKafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * describe:
 * Low-Level Processor API Example
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/29
 */
public class ProcesserAPIStreamExample {

    private static final Logger LOG = LoggerFactory.getLogger(ProcesserAPIStreamExample.class);

    public static void main(String[] args) {

        // build state store
        StateStoreSupplier countStore = Stores.create("Counts-1")
                .withKeys(Serdes.String())
                .withValues(Serdes.Long())
                .persistent()
                .build();
        // build Processor Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", "wordcount-example-1-source")
                .addProcessor("LineCount", LineCountProcessor::new, "SOURCE")
                // add the created state store "Counts-1" associated with processor "LineCount"
                .addStateStore(countStore, "LineCount")
                .addProcessor("WordCount", WordCountProcessor::new, "LineCount")
                .addProcessor("AddTimeStamp", AddTimeStampProcessor::new, "WordCount")
                // connect the state store "Counts-1" with processor "WordCount"
                .connectProcessorAndStateStores("WordCount", "Counts-1")
                .addSink("SINK", "wordcount-example-1-sink", "AddTimeStamp");
        // begin stream
        KafkaStreams streams = new KafkaStreams(builder, loadProp());
        streams.start();
    }

    /**
     * 行数统计
     */
    static class LineCountProcessor implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, Long> kvStore;
        private final static String LINES_COUNT = "lines count";

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            // keep the processor context locally because we need it in punctuate() and commit()
            this.context = context;
            // retrieve the key-value store named "Counts-1"
            this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Counts-1");
        }

        @Override
        public void process(String dummy, String line) {
            // line count
            Long lineNum = this.kvStore.get(LINES_COUNT);
            if (lineNum == null) {
                this.kvStore.put(LINES_COUNT, 1L);
            } else {
                this.kvStore.put(LINES_COUNT, lineNum + 1L);
            }
            context.forward(this.kvStore.get(LINES_COUNT).toString(), line);
        }

        @Override
        public void punctuate(long timestamp) {
            // do nothing
        }

        @Override
        public void close() {
            // close any resources managed by this processor.
            // Note: Do not close any StateStores as these are managed
            // by the library
        }
    }

    /**
     * 词数统计
     */
    static class WordCountProcessor implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, Long> kvStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            // keep the processor context locally because we need it in punctuate() and commit()
            this.context = context;
            // call this processor's punctuate() method every 1000 milliseconds.
            // but this is a bug, will reslove in Kafka v1.0,punctuate() will be Deprecated
            // This is not entirely accurate, as if no data is received for a while, punctuate won't be called.
            // When you receive new data , punctuate will play catch up and will be called many times at reception of the new data.
            //https://issues.apache.org/jira/browse/KAFKA-6092
            //https://github.com/apache/kafka/pull/4301
            this.context.schedule(1000);
            // retrieve the key-value store named "Counts-1"
            this.kvStore = (KeyValueStore<String, Long>) context.getStateStore("Counts-1");
        }

        @Override
        public void process(String lineNum, String line) {
            // word count
            String[] words = line.split(" ");
            for (String word : words) {
                Long oldValue = this.kvStore.get(word);
                if (oldValue == null) {
                    this.kvStore.put(word, 1L);
                } else {
                    this.kvStore.put(word, oldValue + 1L);
                }
            }
            KeyValueIterator<String, Long> iter = this.kvStore.all();
            StringBuilder sb = new StringBuilder();
            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                sb.append(entry.key + "：" + entry.value.toString() + " ");
            }
            context.forward(null, sb.toString());
            iter.close();
        }

        @Override
        public void punctuate(long timestamp) {
            // do nothing
        }

        @Override
        public void close() {
            // close any resources managed by this processor.
            // Note: Do not close any StateStores as these are managed
            // by the library
        }
    }

    /**
     * 消息添加处理时间戳
     */
    static class AddTimeStampProcessor implements Processor<String, String> {

        private ProcessorContext context;

        @Override
        public void init(ProcessorContext processorContext) {
            this.context = processorContext;
        }

        @Override
        public void process(String key, String value) {
            context.forward(key, value + " process_timestamp：" + System.currentTimeMillis());
            // commit the current processing progress
            context.commit();
        }

        @Override
        public void punctuate(long l) {
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }
    }

    /**
     * 加载配置项
     *
     * @return
     */
    public static Properties loadProp() {
        Properties prop = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-example-1");
        // Where to find Kafka broker(s).
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.108:9092,192.168.5.109:9092,192.168.5.110:9092,");
        // Specify default (de)serializers for record keys and for record values.
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        prop.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        return prop;
    }
}
