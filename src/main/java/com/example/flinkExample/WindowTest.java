package com.example.flinkExample;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * @author: chenting
 * @date:2021/04/27 11:30
 */
public class WindowTest {
    public static void main(String[] args) throws Exception{
        //step-1 获取evn
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> source = env.addSource(new SourceFunction<String>() {
            private boolean flag = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    String key = "key" + random.nextInt(3);
                    String value = "value" + random.nextInt(100);
                    ctx.collect("{\"key\": \""+key+"\",\"value\": \""+value+"\"}");
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        //transform
        DataStream<JSONObject> out = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                JSONObject obj = JSONObject.parseObject(value);

                Tuple2<String, String> kv = new Tuple2<>(obj.getString("key"), obj.getString("value"));
                return kv;
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(1))

                .apply(new RichWindowFunction<Tuple2<String, String>, JSONObject, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, String>> input, Collector<JSONObject> out) throws Exception {

                        Iterator<Tuple2<String, String>> iterator = input.iterator();
                        Map<String, Tuple2<String, String>> map = new HashMap<>();
                        while (iterator.hasNext()) {
                            Tuple2<String, String> ele = iterator.next();
                            if (!map.containsKey(ele.f0)) {
                                System.out.printf("key: %s first come,value: ", ele.f0, ele.f1);
                                map.put(ele.f0, ele);
                            } else {
                                Tuple2<String, String> old = map.get(ele.f0);
                                if (ele.f1.length() > old.f1.length()) {
                                    System.out.printf("key: %s old value: %s new value:%s", old.f0, old.f1, ele.f1);
                                    map.put(ele.f0, ele);
                                }
                            }
                        }
                        for (String key : map.keySet()) {
                            JSONObject obj = new JSONObject();
                            obj.put("key", key);
                            obj.put("value", map.get(key));
                            System.out.println("window:" + obj);
                            out.collect(obj);
                        }
                    }
                });

        out.print();

        //step-4 sink
       /* Properties prop2 = new Properties();
        prop2.put("bootstrap.servers","localhost:9092");
        String brokerList = "localhost:9092";
        String topic2 = "test-3";
        FlinkKafkaProducer producer = new FlinkKafkaProducer(brokerList,topic2,new SimpleStringSchema());

        out.addSink(producer);*/

        //step-5 execute
        env.execute();
    }
}
