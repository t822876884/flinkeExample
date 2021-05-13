package com.example.flinkExample;

import com.example.flinkExample.source.KafkaSourceTest;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author bertram
 * @date 2021/4/29 9:57
 * @desc
 */
public class Test1 {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 2. 数据源-source
        DataStream<String> inputDataStream  = env.addSource(new KafkaSourceTest().source("stream"));

        // 3. 数据转换-transformation
/*
数据：
a,3
a,2
a,7
d,9
b,6
a,5
b,3
e,7
e,4
 */
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDataStream = inputDataStream
                .filter(line -> null != line && line.trim().split(",").length == 2)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String line) throws Exception {
                        String[] split = line.trim().split(",");
                        return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
                    }
                });

        // TODO: 先按照卡口分组，再进行窗口操作，最后聚合累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDataStream = mapDataStream
                .keyBy(0) // 下标索引，卡口编号
                // public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size, Time slide)
                .timeWindow(Time.seconds(10),Time.seconds(5))  // TODO: 滑动时间窗口，每5s统计最近10s的数据
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    private FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss") ;

                    @Override
                    public void apply(Tuple tuple, // 表示分组Key，封装到元组中
                                      TimeWindow window, // 时间窗口，获取startTime和endTime
                                      Iterable<Tuple2<String, Integer>> input, // 窗口中数据，进行聚合操作
                                      Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 获取分组Key
                        String  key = (String)((Tuple1)tuple).f0 ;

                        // 获取窗口开始时间和结束时间
                        long start = window.getStart();
                        long end = window.getEnd();

                        // 输出内容
                        String output = "[" + format.format(start) + "~" + format.format(end) + "] -> " + key ;

                        // 对窗口中的数据进行聚合
                        int count = 0 ;
                        for (Tuple2<String, Integer> item: input){
                            count += item.f1 ;
                        }

                        // 最后输出
                        out.collect(Tuple2.of(output, count));
                    }
                });

        // 4. 数据终端-sink
        sumDataStream.printToErr();

        // 5. 触发执行-execute
        env.execute(Test1.class.getSimpleName()) ;
    }

}
