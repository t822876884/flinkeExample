package com.example.flinkExample.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author bertram
 * @date 2021/4/25 19:46
 * @desc
 */
public class Transformation_CF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        OutputTag<Integer> oddTag = new OutputTag<Integer>("奇数", TypeInformation.of(Integer.class));
        OutputTag<Integer> evenTag = new OutputTag<Integer>("偶数", TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> process = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer integer, Context context, Collector<Integer> collector) throws Exception {
                if (integer % 2 == 0) {
                    context.output(oddTag, integer);
                } else {
                    context.output(evenTag, integer);
                }
            }
        });

        DataStream<Integer> evenSource = process.getSideOutput(evenTag);
        DataStream<Integer> oddSource = process.getSideOutput(oddTag);

        evenSource.print("奇数");
        oddSource.print("偶数");

        env.execute("test");

    }
}
