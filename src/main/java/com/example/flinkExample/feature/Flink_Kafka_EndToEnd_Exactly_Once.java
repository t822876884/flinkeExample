package com.example.flinkExample.feature;

import org.apache.commons.lang.SystemUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author bertram
 * @date 2021/5/13 16:40
 * @desc
 */
public class Flink_Kafka_EndToEnd_Exactly_Once {
    public static void main(String[] args) throws Exception {
        //TODO env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 开启checkpoint
        //设置checkpoint的时间间隔为1000ms(每隔1000ms发一次barrier)、设置checkpoint的执行模式为 EXACTLY_ONCE(默认)
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://"));
        }

        //设置两个checkpoint之间的最小等待时间,默认为0
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置可容忍的检查点失败数,默认值为0,意味着不容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        //是否保留外部检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置checkpoint的超时时间，超过时间说明失败，则丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(5, TimeUnit.SECONDS)
        ));

        //TODO source
        Properties props1 = new Properties();
        props1.setProperty("bootstrap.servers", "172.172.177.70:9092,172.172.177.72:9092,172.172.177.73:9092");
        //消费者组id
        props1.setProperty("group.id", "flink");
        //latest：有offset记录就从记录位置开始消费，没有记录从最新的消息开始消费
        props1.setProperty("auto.offset.reset", "latest");
        //动态分区检测
        props1.setProperty("flink.partition-discovery.interval-millis", "5000");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka_source", new SimpleStringSchema(), props1);
        //做checkpoint的时候提交offset到checkpoint(容错) 和 默认主题(外部工具获取)
        kafkaSource.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //TODO transformation
        SingleOutputStreamOperator<String> result = kafkaDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).keyBy(t -> t.f0)
                .sum(1)
                .map((MapFunction<Tuple2<String, Integer>, String>) value -> value.f0 + ":::" + value.f1);

        //TODO sink
        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "172.172.177.70:9092,172.172.177.72:9092,172.172.177.73:9092");
        props2.setProperty("transaction.timeout.ms", "5000");

        FlinkKafkaProducer kafkaSink = new FlinkKafkaProducer<>("flink_kafka_sink",
                new KeyedSerializationSchemaWrapper(new SimpleStringSchema()) {
                },
                props2,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        result.addSink(kafkaSink);

        env.execute();
    }
}
