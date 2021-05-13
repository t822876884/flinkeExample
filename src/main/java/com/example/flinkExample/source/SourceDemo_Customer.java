package com.example.flinkExample.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * @author bertram
 * @date 2021/4/24 15:49
 * @desc
 */
public class SourceDemo_Customer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Order> ds = env.addSource(new MyOrderSource()).setParallelism(1);

        ds.print();

        env.execute("test_collection");

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String id;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }

    public static class MyOrderSource extends RichParallelSourceFunction<Order> {

        private Boolean flag = true;

        //执行并生成数据
        @Override
        public void run(SourceContext<Order> sourceContext) throws Exception {
            Random random = new Random();
            while (flag) {
                String oid = UUID.randomUUID().toString();
                int userId = random.nextInt(3);
                int money = random.nextInt(101);
                long createTime = System.currentTimeMillis();
                sourceContext.collect(new Order(oid, userId, money, createTime));
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
