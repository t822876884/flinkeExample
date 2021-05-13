package com.example.flinkExample.connector;

import com.example.flinkExample.source.KafkaSourceTest;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author bertram
 * @date 2021/4/28 10:43
 * @desc
 */
public class Connector_KafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> kafkaDS  = env.addSource(new KafkaSourceTest().source("stream"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.172.177.70:9092,172.172.177.72:9092,172.172.177.73:9092");
        kafkaDS.addSink(new FlinkKafkaProducer<String>("stream_01",new SimpleStringSchema(),properties));

        env.execute("test_collection");

    }

}
