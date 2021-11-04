package com.xjn.apitest.state;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple3;

/**
 * @author shkstart
 * @create 2021-10-31 18:19
 */
//状态后端
public class State04_StateBackend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //内存级的状态后端
        env.setStateBackend(new MemoryStateBackend());
        //文件系统状态后端(默认)
        env.setStateBackend(new FsStateBackend(""));
        //在RocksDB数据库中存储状态        env.setStateBackend(new RocksDBStateBackend(""));

        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        mapDataStream.print();
        env.execute();
    }
}
