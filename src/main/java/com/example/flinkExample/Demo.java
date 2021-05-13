package com.example.flinkExample;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bertram
 * @date 2021/4/29 14:51
 * @desc
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source


        //TODO 2.transformation


        //TODO 3.sink


        //TODO 4.execute
        env.execute();
    }
}
