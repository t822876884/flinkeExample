package com.example.flinkExample.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author bertram
 * @date 2021/4/29 11:05
 * @desc
 */
public class KafkaSourceTest {
    public FlinkKafkaConsumer source(String topic){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.172.177.70:9092,172.172.177.72:9092,172.172.177.73:9092");
        props.setProperty("group.id", "flink");
        props.setProperty("auto.offset.reset","latest");
        /**会开启一个后台线程每隔5s检测一下Kafka的分区情况 */
        props.setProperty("flink.partition-discovery.interval-millis","5000");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "2000");
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

}
