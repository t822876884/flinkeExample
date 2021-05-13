package com.example.flinkExample.source;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author bertram
 * @date 2021/4/24 15:49
 * @desc
 */
public class SourceDemo_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> ds1 = env.fromElements("hello world", "this world");
        DataStream<String> ds2 = env.fromCollection(Arrays.asList("hello world", "this world"));
        DataStream<Long> ds3 = env.generateSequence(1, 100);
        DataStream<Long> ds4 = env.fromSequence(1, 100);


        ds1.print();
        ds2.print();
        ds3.print();
        ds4.print();

        env.execute("test_collection");

    }
}
