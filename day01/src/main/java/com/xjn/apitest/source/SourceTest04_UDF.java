package com.xjn.apitest.source;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author shkstart
 * @create 2021-10-26 16:26
 */
public class SourceTest04_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> UDFSource = env.addSource(new MySensorSource());
        UDFSource.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading>{

        private boolean running = true;
        Random random = new Random();

        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            while (running){
                for (int i = 0; i < 10; i++) {
                    double temp = random.nextGaussian();
                    sourceContext.collect(new SensorReading("sensor_"+(i+1),System.currentTimeMillis(),temp));
                }
                Thread.sleep(1000);
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
