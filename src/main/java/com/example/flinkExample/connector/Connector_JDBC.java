package com.example.flinkExample.connector;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author bertram
 * @date 2021/4/28 10:43
 * @desc
 */
public class Connector_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<KStreamTest> ds = env.fromElements(new KStreamTest(null,"bertram",18));

        ds.addSink(JdbcSink.sink("INSERT INTO `k_stream_test`(`name`, `age`) VALUES (?, ?)"
                ,(ps,student)->{
                    ps.setString(1,"bertram");
                    ps.setInt(2,18);
                }
                ,new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://172.172.177.54:3306/kstream?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
                        .withUsername("root")
                        .withPassword("test11")
                        .build()));

        env.execute("test_collection");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KStreamTest {
        private Integer id;
        private String name;
        private Integer age;
    }

}
