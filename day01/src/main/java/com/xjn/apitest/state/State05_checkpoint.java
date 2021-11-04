package com.xjn.apitest.state;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shkstart
 * @create 2021-11-02 9:36
 */
public class State05_checkpoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //启动检查点配置，如果不传参，默认每500ms启动一次检查点
        env.enableCheckpointing(1000);
        //和上面参数一样
        env.getCheckpointConfig().setCheckpointInterval(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置检查点设置如果超过60s就终止
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置同一时间能有几个检查点任务并行执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //设置两个检查点任务最小需要间隔多少时间开启
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        //设置恢复策略
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        //设置容忍检查点任务失败的次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
        //重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(2, Time.seconds(10),Time.seconds(2)));
        
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        mapDataStream.print();
        env.execute();
    }
}
