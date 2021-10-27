package com.xjn.apitest.transform;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * @author shkstart
 * @create 2021-10-27 10:28
 */
public class Transform05_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> inputsource = env.readTextFile("D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = mapDataStream.map(new MyMapFunction());
        mapDataStream.print();
        env.execute();
    }

    public static class MyMapFunction extends RichMapFunction<SensorReading, Tuple2<String,Integer>> {

        //初始化工作，一般是定义状态或者建立数据库连接，如果连接数据库放到map中执行会来一个数据连接一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open");
        }
        //一般是关闭连接和清空状态的收尾操作
        @Override
        public void close() throws Exception {
            System.out.println("close");
        }

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return new Tuple2<>(sensorReading.getId(),getRuntimeContext().getIndexOfThisSubtask());

        }
    }
}
