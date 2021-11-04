package com.xjn.apitest.state;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author shkstart
 * @create 2021-10-29 22:24
 */
public class State01_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        mapDataStream.map(new MyMapFunction()).print();
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer>{
        private Integer count = 0;
        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer sta : state) {
                count += sta;
            }
        }
    }
}
