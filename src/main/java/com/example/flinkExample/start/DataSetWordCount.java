package com.example.flinkExample.start;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author bertram
 * @date 2021/4/24 11:13
 * @desc 演示flink-dataset-api 实现wordCount
 */
public class DataSetWordCount {
    public static void main(String[] args) throws Exception {
        //todo env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //todo source
        DataSet<String> source = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        //todo transformation
        DataSet<String> worlds = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //value 表示每一行数据
                String[] arr = value.split(" ");
                for (String world : arr) {
                    out.collect(world);
                }
            }
        });

        DataSet<Tuple2<String, Integer>> wordAndOne = worlds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        //todo sink
        result.print();
    }
}
