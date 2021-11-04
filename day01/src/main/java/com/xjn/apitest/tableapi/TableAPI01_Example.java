package com.xjn.apitest.tableapi;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author shkstart
 * @create 2021-11-03 20:09
 */
public class TableAPI01_Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputsource = env.readTextFile("D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        //基于流创建Table环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //使用table API 先将流转化为table类型,再调用table API进行转换操作
        Table dataTable = tableEnv.fromDataStream(mapDataStream);
        Table resultTable = dataTable.select("id,temperature").where("id='sensor_1'");

        //使用Flink SQL
        tableEnv.createTemporaryView("sensor",dataTable);
        String sql = "select id,temperature from sensor where id='sensor_1'";
        Table tableSqlResult = tableEnv.sqlQuery(sql);

        //Flink sql和table API都需要转换为流执行
        tableEnv.toAppendStream(resultTable, Row.class).print("TableAPI");
        tableEnv.toAppendStream(tableSqlResult,Row.class).print("FlinkSql");

        env.execute();

    }
}
