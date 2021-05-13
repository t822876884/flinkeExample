package com.example.flinkExample.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author bertram
 * @date 2021/4/29 14:51
 * @desc
 */
public class Case01 {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        //TODO 1.source
        DataStream<Order> dataStreamA = bsEnv.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)

        ));

        DataStream<Order> dataStreamB = bsEnv.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        //TODO 2.transformation
        bsTableEnv.createTemporaryView("tableA",dataStreamA,$("user"),$("product"),$("amount"));
        Table tableB = bsTableEnv.fromDataStream(dataStreamB, $("user"), $("product"), $("amount"));
        System.out.println(tableB.getSchema());

        Table tableResult = bsTableEnv.sqlQuery("select * from tableA where amount > 2 union all select * from " + tableB + " where amount > 2");
//        Table tableResult = bsTableEnv.sqlQuery(
//                "SELECT * FROM tableA WHERE amount > 2 " +
//                        "UNION ALL " +
//                        "SELECT * FROM " + tableB + " WHERE amount < 2"
//        );

        //TODO 3.sink
        DataStream<Tuple2<Boolean, Order>> resultStream = bsTableEnv.toRetractStream(tableResult, Order.class);
        resultStream.print();

        //TODO 4.execute
        bsEnv.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        public Long user;
        public String product;
        public int amount;
    }

}
