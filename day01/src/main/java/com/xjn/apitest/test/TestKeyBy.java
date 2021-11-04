package com.xjn.apitest.test;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shkstart
 * @create 2021-10-29 15:17
 */

//keyby即其他分区器决定了当前数据进到下游哪个分区，
//用自己的hashcode对下游算子并行度取模，可以确保相同key的数据进行同一slot
public class TestKeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> inputsource = env.readTextFile("D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> dataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        dataStream.keyBy("id").print();
        env.execute();
    }
}
