package com.xjn.apitest.transform;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shkstart
 * @create 2021-10-26 20:23
 */
public class Transform02_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //为了保证有序输出
        env.setParallelism(1);
        DataStream<String> inputsource = env.readTextFile("D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt");
        //map封装数据
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        //分组
//        KeyedStream<SensorReading, Tuple> keyedStream = mapDataStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream1 = mapDataStream.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });
        //滚动聚合
        //max只有排序的字段会改变，其他字段不变，需要用maxby
//        keyedStream1.max("temperature").print();
        keyedStream1.maxBy("temperature").print();

        env.execute();
    }
}
