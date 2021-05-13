package com.example.flinkExample.start;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author bertram
 * @date 2021/4/24 11:39
 * @desc
 */
public class FlinkWordCount_YARN {
    public static void main(String[] args) throws Exception {
        //todo env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //todo source
        DataStreamSource<String> source = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        //todo transformation
        DataStream<String> words = source
                .flatMap((String value, Collector<String> out) -> Arrays.asList(value.split(" ")).forEach(out::collect))
                .returns(Types.STRING);

        DataStream<Tuple2<String, Integer>> wordAndOne = words
                .map((String value) -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        KeyedStream<Tuple2<String, Integer>, String> keyByed = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyByed.sum(1);

        //todo sink
        result.print();

        //todo commit and execute
        env.execute("stream");
    }
}
