package com.xjn.apitest.processfunction;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author shkstart
 * @create 2021-11-01 16:43
 */
public class Process03_SideOutputCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.socketTextStream("hadoop102", 7777);
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        OutputTag<SensorReading> lowTemp = new OutputTag<SensorReading>("lowTemp"){
        };
        SingleOutputStreamOperator<SensorReading> highTemp = mapDataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.getTemperature() > 30 ){
                    out.collect(value);
                }else {
                    ctx.output(lowTemp,value);
                }
            }
        });
        highTemp.print("highTemp");
        highTemp.getSideOutput(lowTemp).print("lowTemp");
        env.execute();
    }
}
