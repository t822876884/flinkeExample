package com.example.flinkExample.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bertram
 * @date 2021/4/28 10:20
 * @desc
 */
public class Sink_console {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<String> longDs = env.readTextFile("data/input/test.text");


        /*longDs.print();
        longDs.print("带标识输出");*/
        longDs.writeAsText("data/output/result1").setParallelism(1);
        longDs.writeAsText("data/output/result2").setParallelism(2);
        longDs.rebalance().writeAsText("data/output/result3").setParallelism(2);

        env.execute("sinkTest");
    }
}
