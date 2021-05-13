package com.example.flinkExample.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author bertram
 * @date 2021/4/29 14:51
 * @desc
 */
public class Case03 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env, bsSettings);

        //TODO 1.source
        DataStreamSource<Order> orderDS = env.addSource(new RichSourceFunction<Order>() {
            private Boolean flag = true;

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                Random random = new Random();
                while (flag) {
                    Order order = new Order(UUID.randomUUID().toString(), random.nextInt(3), random.nextInt(101), System.currentTimeMillis());
                    TimeUnit.SECONDS.sleep(1);
                    ctx.collect(order);
                }
            }

            @Override
            public void cancel() {
                flag = false;
            }
        });

        //TODO 2.transformation
        DataStream<Order> watermarksDs = orderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((order, timestamp) -> order.getCreateTime()));


        //TODO DataStream转Table or View
        tbEnv.createTemporaryView("t_order",watermarksDs,$("orderId"), $("userId"), $("money"), $("createTime").rowtime());

        String SQL = "SELECT " +
                "userId," +
                "COUNT(1) AS totalCount," +
                "MAX(money) AS maxMoney," +
                "MIN(money) AS minMoney  " +
                "FROM t_order " +
                "GROUP BY userId, TUMBLE(createTime, INTERVAL '5' SECOND)";
        Table table = tbEnv.sqlQuery(SQL);

        //TODO Table or View转DataStream
        DataStream<Tuple2<Boolean, Row>> resultDs = tbEnv.toRetractStream(table, Row.class);
        //TODO 3.sink
        resultDs.print();

        //TODO 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        private String orderId;
        private Integer userId;
        private Integer money;
        private Long createTime;
    }


}
