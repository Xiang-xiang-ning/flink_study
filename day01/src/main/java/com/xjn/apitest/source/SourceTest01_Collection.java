package com.xjn.apitest.source;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author shkstart
 * @create 2021-10-26 14:15
 */
public class SourceTest01_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
        DataStream<SensorReading> dataStream1 = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));
        DataStream<Integer> dataStream2 = env.fromElements(1, 4, 3, 76, 43);
        dataStream1.print("sensor");
        dataStream2.print("number");
        env.execute();
    }
}
