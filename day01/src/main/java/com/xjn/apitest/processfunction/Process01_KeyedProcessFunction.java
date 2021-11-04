package com.xjn.apitest.processfunction;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/**
 * @author shkstart
 * @create 2021-10-31 18:51
 */
public class Process01_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        mapDataStream.keyBy("id").process(new MyProcessFunction()).print();

        env.execute();
    }

    public static class MyProcessFunction extends KeyedProcessFunction<Tuple,SensorReading,Integer> {

        private ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("定时器",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            ctx.timestamp();
            ctx.getCurrentKey();
            //侧输出流
//            ctx.output();
            ctx.timerService().currentWatermark();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+5000);
            valueState.update(ctx.timerService().currentProcessingTime()+5000);
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp()+10)*100);
            ctx.timerService().deleteProcessingTimeTimer(valueState.value());
            out.collect(value.getId().length());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp+"定时器任务执行");;
        }

        @Override
        public void close() throws Exception {
            valueState.clear();
        }
    }
}
