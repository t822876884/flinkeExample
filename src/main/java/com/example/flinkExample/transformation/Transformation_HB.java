package com.example.flinkExample.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
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
public class Transformation_HB {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);


        DataStream<String> ds1 = env.fromElements("hadoop1", "spark1", "flink1");
        DataStream<String> ds2 = env.fromElements("hadoop2", "spark2", "flink2");
        DataStream<String> ds4 = env.fromElements("hadoop4", "spark4", "flink4");
        DataStream<Long> ds3 = env.fromElements(1L, 2L, 3L);

        /*DataStream<String> union = ds1.union(ds2).union(ds4);
        //ds1.union(ds3);
        //ds1.connect(ds2);


        union.print();*/

            ConnectedStreams<String, Long> connect = ds1.connect(ds3);
            SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String s) throws Exception {
                return "String:" + s;
            }

            @Override
            public String map2(Long aLong) throws Exception {
                return "Long:" + aLong;
            }
        });

        result.print();

        env.execute("test");

    }
}
