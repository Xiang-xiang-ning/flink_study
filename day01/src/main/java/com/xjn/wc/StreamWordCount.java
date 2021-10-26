package com.xjn.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shkstart
 * @create 2021-10-25 11:33
 */

//流处理wordcount
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        String path = "D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\wordcount";
//        DataStream<String> dataStream = env.readTextFile(path);

        env.setParallelism(2);

        //从edit configuration中传参
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //linux通过nc -lk 7777发送文本流
        DataStream<String> dataStream = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dataStream.flatMap(new WordCount.FlatMapper());
        flatMap.keyBy(0)
                .sum(1).print().setParallelism(3);
        env.execute();
    }
}
