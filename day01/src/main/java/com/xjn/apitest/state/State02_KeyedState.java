package com.xjn.apitest.state;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shkstart
 * @create 2021-10-29 22:40
 */
public class State02_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        mapDataStream.keyBy("id").map(new MyMap()).print();

        env.execute();
    }

    public static class MyMap extends RichMapFunction<SensorReading, Integer> {
        private ValueState<Integer> valueState;
        private ListState<String> listState;
        private MapState<String,Double> myMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my_value",Integer.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my_list",String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            //ValueState
            Integer count = valueState.value();
            count++;
            valueState.update(count);

            //MapState
            Double value = myMapState.get("1");
            myMapState.put("2",10.1);
            myMapState.remove("2");
            myMapState.clear();

            //ListState
            for (String str : listState.get()) {
                System.out.println(str);
            }
            listState.add("hello");

            return count;
        }
    }
}
