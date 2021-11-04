package com.xjn.apitest.window;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author shkstart
 * @create 2021-10-28 16:10
 */
public class Window03_WaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //将时间语义设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        //做watermark测试的时候注意并行度
        env.setParallelism(4);

        DataStream<String> inputsource = env.socketTextStream("hadoop102",7777);
        //map封装数据
        DataStream<SensorReading> dataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        })

        //有序数据设置事件时间和watermark
//        mapDataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//            @Override
//            public long extractAscendingTimestamp(SensorReading sensorReading) {
//                return sensorReading.getTimestamp()*1000L;
//            }
//        });
        //乱序数据设置时间戳和watermark
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp() * 1000L;
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        DataStream<SensorReading> minTemp =
                keyedStream
                .timeWindow(Time.seconds(15))
                .minBy("temperature");


        dataStream.print("mapstream");
        keyedStream.print("keyedstream");
        minTemp.print("minTemp");
        env.execute();
    }
}
