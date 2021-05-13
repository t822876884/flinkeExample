package com.example.flinkExample.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author bertram
 * @date 2021/4/25 19:46
 * @desc
 */
public class Transformation_otherPartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        DataStream<String> longDs = env.readTextFile("src/main/resources/test.text");
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatDs = longDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String word : s) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

//        DataStream<Tuple2<String, Integer>> result = flatDs.global();
//        DataStream<Tuple2<String, Integer>> result = flatDs.broadcast();
//        DataStream<Tuple2<String, Integer>> result = flatDs.forward();
//        DataStream<Tuple2<String, Integer>> result = flatDs.shuffle();
//        DataStream<Tuple2<String, Integer>> result = flatDs.rescale();
        DataStream<Tuple2<String, Integer>> result = flatDs.partitionCustom(new MyPartitioner(), t -> t.f0);

        result.print();

        env.execute("test");

    }

    private static class MyPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            return 2;
        }
    }
}
