package com.xjn.apitest.processfunction;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author shkstart
 * @create 2021-11-01 10:01
 */
public class Process02_ApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        mapDataStream.keyBy("id").process(new MyProcessCase(10)).print();
        env.execute();
    }

    public static class MyProcessCase extends KeyedProcessFunction<Tuple,SensorReading,String>{
        //设置统计的间隔时间
        private Integer interval;
        //用于存放前一个温度状态
        private ValueState<Double> tempState;
        //存放定时器时间戳(删除定时器时需要)
        private ValueState<Long> timerState;

        public MyProcessCase(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp_state",Double.class,Double.MIN_VALUE));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer_state",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double currentTemp = value.getTemperature();
            //如果当前温度比之前温度高，且没有注册定时器任务时
            if (currentTemp >= tempState.value() && timerState.value() == null){
                Long currentTime = ctx.timerService().currentProcessingTime()+interval*1000;
                //注册指定时长的定时器
                ctx.timerService().registerProcessingTimeTimer(currentTime);
                //保存定时器时间状态
                timerState.update(currentTime);
            }
            //如果当前温度小于之前温度，且已经注册了定时器，需要删除定时器，且清空时间状态
            else if (currentTemp < tempState.value() && timerState.value() != null){
                ctx.timerService().deleteProcessingTimeTimer(timerState.value());
                timerState.clear();
            }
            //不论温度如何，都要更新温度状态
            tempState.update(currentTemp);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器"+ctx.getCurrentKey().getField(0)+"温度值连续增长"+interval+"s!");
            //每一个定时器任务触发后就要把时间状态清空，等待下一个定时器任务创建
            timerState.clear();
        }

        @Override
        public void close() throws Exception {
            tempState.clear();
        }
    }
}
