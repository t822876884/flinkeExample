package com.example.flinkExample.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Iterator;

/**
 * @author bertram
 * @date 2021/4/29 14:47
 * @desc broadcastState
 */
public class StateDemo_OperatorState {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        env.setParallelism(1);
        //先直接使用下面的代码设置Checkpoint时间间隔和磁盘路径以及代码遇到异常后的重启策略
        env.enableCheckpointing(1000);//每隔1s执行一次Checkpoint
        env.setStateBackend(new FsStateBackend("file:///D:/ckp"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //固定延迟重启策略: 程序出现异常的时候，重启2次，每次延迟3秒钟重启，超过2次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 3000));

        //TODO 1.source
        DataStreamSource<String> ds = env.addSource(new MyKafkaSource()).setParallelism(1);

        //TODO 2.transformation


        //TODO 3.sink
        ds.print();

        //TODO 4.execute
        env.execute();
    }

    //使用OperatorState的ListState模拟Kafkasource进行offset维护
    public static class MyKafkaSource extends RichParallelSourceFunction<String> implements CheckpointedFunction{
        //声明一个OperatorState的ListState来记录offset
        private ListState<Long> offsetState = null;
        private Long offset = 0L;

        private boolean flag = true;

        //初始化 ListState
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //创建状态描述符
            ListStateDescriptor<Long> descriptor = new ListStateDescriptor<Long>("offsetState",Long.class);
            //根据状态描述符获取state
            offsetState = context.getOperatorStateStore().getListState(descriptor);
        }

        //使用ListState，获取state的值
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (flag) {
                Iterator<Long> iterator = offsetState.get().iterator();
                if (iterator.hasNext()){
                    offset = iterator.next();
                }
                offset += 1;
                int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
                ctx.collect("subTaskId："+subTaskId+"当前offset值为:"+offset);
                Thread.sleep(1000);
                if (offset % 5 == 0){
                    throw new Exception("bug ...");
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        //保存到checkpoint
        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            offsetState.clear();//先清理内存中的状态，并保存到Chekpoint文件夹中
            offsetState.add(offset);//将当前offset保存到offsetState中
        }
    }
}
