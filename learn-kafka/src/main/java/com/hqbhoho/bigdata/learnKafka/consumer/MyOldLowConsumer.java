package com.hqbhoho.bigdata.learnKafka.consumer;


import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * describe:
 * Kafka 旧生产者客户端低级API
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/03/17
 */
public class MyOldLowConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(MyOldHighConsumer.class);

    private List<String> m_replicaBrokers = new ArrayList<String>();

    public static void main(String[] args) {
        MyOldLowConsumer example = new MyOldLowConsumer();
        List<String> seeds = new ArrayList<>();
        seeds.add("192.168.5.108");
        seeds.add("192.168.5.109");
        seeds.add("192.168.5.110");
        try {
            example.run(10, "testTopic001", 1, seeds, 9092);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 消费者主要运行过程
    public void run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) throws Exception {

        // 寻找分区的主副本
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {
            LOG.error("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            LOG.error("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + a_topic + "_" + a_partition;

        //构建SimpleConsumer，底层与Kafka broker交互
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
        // 向分区获取最早的偏移量
        long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

        int numErrors = 0;
        // 限制读取的数据量
        while (a_maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
            }
            //  构建请求的时候  指定了readOffset  可以从用户指定的offset开始读取数据
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    // 合理设置fetchSize
                    .addFetch(a_topic, a_partition, readOffset, 1000)
                    .build();
            //发送消费消息的请求
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                LOG.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                //  允许失败次数是3
                if (numErrors > 3) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                // consumer is invalid we need find new leader
                consumer.close();
                consumer = null;
                // find new valid leader
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                continue;
            }
            // 读取消息
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    LOG.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                LOG.info(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                a_maxReads--;
                if (a_maxReads == 0) break;
            }
        }
        // 要求的数据量消费完  关闭consumer
        if (consumer != null) consumer.close();
    }

    // 当分区主副本节点发生故障时，客户端需要找到故障转移后的主副本
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    // 获取分区读取偏移量
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            LOG.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    // 找寻分区主副本
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");

                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();

                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}
