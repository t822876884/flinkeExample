package com.example.flinkExample.window;

import com.example.flinkExample.source.KafkaSourceTest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author bertram
 * @date 2021/4/28 14:23
 * @desc
 */
public class WindowDemo_Time {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> kafkaDS  = env.addSource(new KafkaSourceTest().source("stream"));

        SingleOutputStreamOperator<Car> carDs = kafkaDS.map(new MapFunction<String, Car>() {
            @Override
            public Car map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Car(arr[0], Integer.parseInt(arr[1]));
            }
        });


        //KeyedStream<Car, Integer> keyByDs = carDs.keyBy(t -> t.count);
        KeyedStream<Car, Integer> keyByDs = carDs.keyBy(Car::getCount);

//        SingleOutputStreamOperator<Car> result = keyByDs.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .sum("count");
        SingleOutputStreamOperator<Car> result = keyByDs.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("count");

        result.print();

        env.execute("test_collection");
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Car {
        private String singleNum;
        private Integer count;
    }
}
