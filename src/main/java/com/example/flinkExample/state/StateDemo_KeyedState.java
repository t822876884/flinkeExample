package com.example.flinkExample.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bertram
 * @date 2021/4/29 14:47
 * @desc 使用KeyedState中的ValueState获取数据流中的最大值(实际中可以使用maxBy即可)
 */
public class StateDemo_KeyedState {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<Tuple2<String, Long>> source = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 9L),
                Tuple2.of("上海", 4L),
                Tuple2.of("北京", 2L),
                Tuple2.of("上海", 19L),
                Tuple2.of("北京", 5L)
        );

        //TODO 2.transformation
        //maxBy
        SingleOutputStreamOperator<Tuple2<String, Long>> result1 = source.keyBy(t -> t.f0).max(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> result2 = source.keyBy(t -> t.f0).maxBy(1);
        //KeyedState
        SingleOutputStreamOperator<Tuple3<String, Long, Long>> result3 = source.keyBy(t -> t.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {

            //定义一个状态来存放最大值
            private ValueState<Long> maxValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创建一个状态描述符对象
                ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("maxValueState", Long.class);
                //根据状态描述符获取(初始化)状态
                maxValueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                //当前状态
                Long current = value.f1;

                //获取状态管理器中存的状态值
                Long historyValue = maxValueState.value();

                //更新状态：初始化         当前值大于历史值
                if (historyValue == null || historyValue < current) {
                    maxValueState.update(current);
                }
                return Tuple3.of(value.f0, current, historyValue);
            }
        });


        //TODO 3.sink
        result1.print("max");
        result2.print("maxBy");
        result3.print("KeyedState");

        //TODO 4.execute
        env.execute();
    }
}
