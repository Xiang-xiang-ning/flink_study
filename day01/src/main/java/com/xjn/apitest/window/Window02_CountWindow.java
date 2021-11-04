package com.xjn.apitest.window;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shkstart
 * @create 2021-10-27 19:51
 */
public class Window02_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        KeyedStream<SensorReading, Tuple> keyedStream = mapDataStream.keyBy("id");
        SingleOutputStreamOperator<Double> resultStream = keyedStream.countWindow(10, 2).aggregate(new AggregateFunction<SensorReading, Tuple2<Integer, Double>, Double>() {
            @Override
            public Tuple2<Integer, Double> createAccumulator() {
                return new Tuple2<Integer, Double>(0, 0.0);
            }

            @Override
            public Tuple2<Integer, Double> add(SensorReading sensorReading, Tuple2<Integer, Double> integerDoubleTuple2) {
                return new Tuple2<>(integerDoubleTuple2.f0 + 1, integerDoubleTuple2.f1 + sensorReading.getTemperature());
            }

            @Override
            public Double getResult(Tuple2<Integer, Double> integerDoubleTuple2) {
                return integerDoubleTuple2.f1 / integerDoubleTuple2.f0;
            }

            @Override
            public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> integerDoubleTuple2, Tuple2<Integer, Double> acc1) {
                return new Tuple2<>(integerDoubleTuple2.f0 + acc1.f0, integerDoubleTuple2.f1 + acc1.f1);
            }
        });
        resultStream.print();
        env.execute();
    }
}
