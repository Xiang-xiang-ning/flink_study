package com.xjn.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author shkstart
 * @create 2021-10-25 10:48
 */

//批处理wordcount
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境，相当于spark中的sparkcontext
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String path = "D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\wordcount";
        //读取解析文件
        DataSource<String> dataSource = environment.readTextFile(path);
        //将输入数据打散，需要自定义函数实现对应接口
        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = dataSource.flatMap(new FlatMapper());
        //按照元组中string分组，按照元组中integer聚合
        flatMap.groupBy(0).sum(1).print();
    }

    static class FlatMapper implements FlatMapFunction<String,Tuple2<String,Integer>>{

        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] split = value.split(" ");
            for (String s : split) {
                collector.collect(new Tuple2<String, Integer>(s,1));
            }
        }
    }
}
