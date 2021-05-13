package com.example.flinkExample.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author bertram
 * @date 2021/4/25 19:46
 * @desc
 */
public class Transformation_Rebalance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        DataStream<Long> longDs = env.fromSequence(0,100);

        SingleOutputStreamOperator<Long> filterDs = longDs.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 10;
            }
        });

        /*SingleOutputStreamOperator<Tuple2<Integer, Integer>> result1 = filterDs.map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(subTaskId, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        result1.print();*/

        SingleOutputStreamOperator<Tuple2<Integer, Integer>> result2 = filterDs.rebalance().map(new RichMapFunction<Long, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Long value) throws Exception {
                int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(subTaskId, 1);
            }
        }).keyBy(t -> t.f0).sum(1);

        result2.print();

        env.execute("test");

    }
}
