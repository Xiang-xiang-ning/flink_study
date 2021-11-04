package com.xjn.apitest.window;

import com.xjn.apitestbean.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;

/**
 * @author shkstart
 * @create 2021-10-27 18:55
 */
public class Window01_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        KeyedStream<SensorReading, Tuple> keyedStream = mapDataStream.keyBy("id");
        //增量聚合函数
        SingleOutputStreamOperator<Integer> aggregate = keyedStream.timeWindow(Time.seconds(20)).aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(SensorReading sensorReading, Integer integer) {
                return integer + 1;
            }

            @Override
            public Integer getResult(Integer integer) {
                return integer;
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return integer + acc1;
            }
        });
//        aggregate.print();

        //全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> resultStream = keyedStream.timeWindow(Time.seconds(15)).apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
            //tuple中是存储key字段
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String,Long,Integer>> collector) throws Exception {
                int size = IteratorUtils.toList(iterable.iterator()).size();
                collector.collect(Tuple3.of(tuple.getField(0),timeWindow.getEnd(),size));
            }
        });
        resultStream.print();

        env.execute();
    }
}
