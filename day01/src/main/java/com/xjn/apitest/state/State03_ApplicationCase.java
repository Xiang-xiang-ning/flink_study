package com.xjn.apitest.state;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * @author shkstart
 * @create 2021-10-30 21:43
 */
public class State03_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> outStream = mapDataStream.keyBy("id").flatMap(new MyFlatMap(10.0));
        outStream.print();

        env.execute();
    }

    public static class MyFlatMap extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

        private Double threshold;
        private ValueState<Double> lastTemp;

        public MyFlatMap(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",Double.class));
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> collector) throws Exception {
            Double last =  lastTemp.value();
            if (last != null){
                Double diff = Math.abs(last- sensorReading.getTemperature());
                if (diff >= threshold){
                    collector.collect(new Tuple3<>(sensorReading.getId(),last, sensorReading.getTemperature()));
                }
            }
            lastTemp.update(sensorReading.getTemperature());
        }
    }
}
