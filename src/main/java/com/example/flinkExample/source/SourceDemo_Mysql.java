package com.example.flinkExample.source;

import com.example.flinkExample.source.SourceDemo_Mysql.KStreamUser;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

/**
 * @author bertram
 * @date 2021/4/24 15:49
 * @desc
 */
public class SourceDemo_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<KStreamUser> ds = env.addSource(new MysqlKStreamUserSource()).setParallelism(1);

        ds.print();

        env.execute("test_collection");

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class KStreamUser {
        private Integer id;
        private Integer dbId;
        private Integer datasourceId;
        private String tableName;
        private Integer type;
        private String note;
        private String responser;
        private String creater;
        private String updater;
        private Date createTime;
        private Date updateTime;
        private Integer status;
        private String preViewSql;
        private Integer isDraft;
        private Integer isWideTable;
        private Integer isOld;

    }

    public static class MysqlKStreamUserSource extends RichParallelSourceFunction<KStreamUser> {

        private Boolean flag = true;
        private static Connection conn = null;
        private static PreparedStatement ps = null;
        private static ResultSet rs = null;

        @Override
        public void run(SourceContext sourceContext) throws Exception {

            while (flag) {
                Thread.sleep(5000);
                rs = ps.executeQuery();

                while (rs.next()) {
                    Integer id = rs.getInt("id");
                    Integer dbId = rs.getInt("dbId");
                    Integer datasourceId = rs.getInt("datasourceId");
                    String tableName = rs.getString("tableName");
                    Integer type = rs.getInt("type");
                    String note = rs.getString("note");
                    String responser = rs.getString("responser");
                    String creater = rs.getString("creater");
                    String updater = rs.getString("updater");
                    Date createTime = rs.getDate("createTime");
                    Date updateTime = rs.getDate("updateTime");
                    Integer status = rs.getInt("status");
                    String preViewSql = "";
                    Integer isDraft = rs.getInt("isDraft");
                    Integer isWideTable = rs.getInt("isWideTable");
                    Integer isOld = rs.getInt("isOld");
                    sourceContext.collect(new KStreamUser(id, dbId, datasourceId, tableName, type, note, responser
                            , creater, updater, createTime, updateTime, status, preViewSql, isDraft, isWideTable, isOld));
                }

            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        //只执行一次，开启资源
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://172.172.177.54:3306/kmeta?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true",
                    "root", "test11");
            ps = conn.prepareStatement("select * from t_lemon_tables where responser='19185409'");
        }

        //关闭资源
        @Override
        public void close() throws Exception {
            if (conn != null) {
                conn.close();
            }
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
    }


}
