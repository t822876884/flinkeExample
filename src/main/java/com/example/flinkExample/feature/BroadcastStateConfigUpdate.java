package com.example.flinkExample.feature;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author bertram
 * @date 2021/5/12 10:23
 * @desc
 */
public class BroadcastStateConfigUpdate {

    public static void main(String[] args) throws Exception {
        //todo env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //todo source
        //构建实时事件流
        DataStreamSource<Tuple4<String, String, String, Integer>> eventDS = env.addSource(new MySource());

        //构建实时配置流
        DataStreamSource<Map<String, Tuple2<String, Integer>>> configDS = env.addSource(new MysqlSource());

        //todo transformation
        //todo 1、定义状态描述器
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor
                                                                        //map(userId, Tuple2.of(userName, userAge));
                = new MapStateDescriptor<>("config", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        //todo 2、广播配置流
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastDS = configDS.broadcast(descriptor);

        //todo 3、将事件流和配置流进行连接
        //Tuple4.of(userId, eventTime, eventType, productId)              map(userId, Tuple2.of(userName, userAge))
        BroadcastConnectedStream<Tuple4<String, String, String, Integer>, Map<String, Tuple2<String, Integer>>> connectDS = eventDS.connect(broadcastDS);

        //todo 4、处理连接后的流
        //BroadcastProcessFunction<IN1, IN2, OUT>
        SingleOutputStreamOperator<Tuple6<String, String, String, Integer, String, Integer>> result = connectDS.process(new BroadcastProcessFunction<
                //事件流 <userID, eventTime, eventType, productID
                Tuple4<String, String, String, Integer>,
                //配置流 <用户id,<姓名,年龄>>
                Map<String, Tuple2<String, Integer>>,
                //处理后的数据流 <用户id，eventTime，eventType，productID，姓名，年龄>
                Tuple6<String, String, String, Integer, String, Integer>>() {

            //处理事件流中的数据  将事件流中的数据与配置流做关联
            @Override
            public void processElement(Tuple4<String, String, String, Integer> value, ReadOnlyContext ctx,
                                       Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {
                String userId = value.f0;
                //根据状态描述器获取广播状态
                ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                if (broadcastState != null) {
                    Map<String, Tuple2<String, Integer>> map = broadcastState.get(null);
                    if (map != null) {
                        Tuple2<String, Integer> tuple2 = map.get(userId);

                        String userName = tuple2.f0;
                        Integer userAge = tuple2.f1;
                        out.collect(Tuple6.of(userId, value.f1, value.f2, value.f3, userName, userAge));
                    }
                }

            }

            @Override
            public void processBroadcastElement(Map<String, Tuple2<String, Integer>> value, Context ctx,
                                                Collector<Tuple6<String, String, String, Integer, String, Integer>> out) throws Exception {
                //根据状态描述器获取广播状态
                BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                //清空历史状态数据
                broadcastState.clear();
                //将最新的配置数据放到state中
                broadcastState.put(null, value);
            }
        });

        //todo sink
        result.print();

        //todo execute
        env.execute();
    }

    /**
     * 事件流
     */
    public static class MySource implements SourceFunction<Tuple4<String, String, String, Integer>> {
        private boolean isRunning = true;
        @Override
        public void run(SourceContext<Tuple4<String, String, String, Integer>> ctx) throws Exception {
            Random random = new Random();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while (isRunning) {
                int id = random.nextInt(4) + 1;
                String userId = "user_"+id;
                String eventTime = sdf.format(new Date());
                String eventType = "type_"+random.nextInt(3);
                int productId = random.nextInt(4);
                ctx.collect(Tuple4.of(userId, eventTime, eventType, productId));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 配置流
     * DROP TABLE IF EXISTS `user_info`;
     * CREATE TABLE `user_info`  (
     *   `userID` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
     *   `userName` varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
     *   `userAge` int(11) NULL DEFAULT NULL,
     *   PRIMARY KEY (`userID`) USING BTREE
     * ) ENGINE = MyISAM CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
     * -- ----------------------------
     * -- Records of user_info
     * -- ----------------------------
     * INSERT INTO `user_info` VALUES ('user_1', '张三', 10);
     * INSERT INTO `user_info` VALUES ('user_2', '李四', 20);
     * INSERT INTO `user_info` VALUES ('user_3', '王五', 30);
     * INSERT INTO `user_info` VALUES ('user_4', '赵六', 40);
     * SET FOREIGN_KEY_CHECKS = 1;
     */
    public static class MysqlSource extends RichSourceFunction<Map<String, Tuple2<String, Integer>>>{
        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;
        private boolean flag = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://172.172.177.54:3306/kmeta?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true",
                    "root", "test11");
            ps = conn.prepareStatement("select `userID`, `userName`, `userAge` from `user_info`");
        }

        @Override
        public void run(SourceContext<Map<String, Tuple2<String, Integer>>> ctx) throws Exception {
            while (flag){
                HashMap<String, Tuple2<String, Integer>> map = new HashMap<>();
                rs = ps.executeQuery();
                while (rs.next()){
                    String userId = rs.getString("userId");
                    String userName = rs.getString("userName");
                    int userAge = rs.getInt("userAge");
                    map.put(userId, Tuple2.of(userName, userAge));
                    Thread.sleep(5000); //每隔5s更新用户配置信息
                }
                ctx.collect(map);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if (conn != null){
                conn.close();
            }
            if (ps != null){
                ps.close();
            }
            if (rs != null){
                rs.close();
            }
        }
    }

}
