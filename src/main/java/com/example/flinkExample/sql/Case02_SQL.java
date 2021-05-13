package com.example.flinkExample.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author bertram
 * @date 2021/4/29 14:51
 * @desc
 */
public class Case02_SQL {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(env,settings);

        //TODO 1.source
        DataStream<WordCount> wcDs = env.fromCollection(Arrays.asList(
                new WordCount("Hello",1),
                new WordCount("Word",1),
                new WordCount("Hello",1)
        ));

        //TODO 2.transformation
        //DataStream 转为 View
        tbEnv.createTemporaryView("t_wc",wcDs,$("word"),$("frequency"));
        String sql = "select word,sum(frequency) as frequency from t_wc group by word";
        Table tableResult = tbEnv.sqlQuery(sql);
        //TODO 3.sink

        //Table 转为 DataStream
//        Append Mode: 仅当动态 Table 仅通过INSERT更改进行修改时，才可以使用此模式，即，它仅是追加操作，并且之前输出的结果永远不会更新。
//        Retract Mode: 任何情形都可以使用此模式。它使用 boolean 值对 INSERT 和 DELETE 操作的数据进行标记。
        DataStream<Tuple2<Boolean, WordCount>> SQLResult = tbEnv.toRetractStream(tableResult, WordCount.class);
//        DataStream<WordCount> SQLResult = tbEnv.toAppendStream(tableResult, WordCount.class);
//        toAppendStream doesn't support consuming update changes which is produced by node GroupAggregate
        SQLResult.print();

        //TODO 4.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WordCount {
        public String word;
        public int frequency;
    }
}
