package com.xjn.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author shkstart
 * @create 2021-10-26 18:55
 */
public class Transform01_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputsource = env.readTextFile("D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt");
        //将读取的字符串转换为长度
        SingleOutputStreamOperator<Integer> mapdatestream = inputsource.map(new MapFunction<String, Integer>() {
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
        //将每一行以逗号分割打散
        SingleOutputStreamOperator<String> FlatmapDataStream = inputsource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });
        //将id为sensor_1的过滤
        SingleOutputStreamOperator<String> filterDataStream = inputsource.filter(new FilterFunction<String>() {
            public boolean filter(String s) throws Exception {
                String[] split = s.split(",");
                if (split[0].equals("sensor_1")){
                    return false;
                }
                return true;
            }
        });

        filterDataStream.print();
        env.execute();
    }
}
