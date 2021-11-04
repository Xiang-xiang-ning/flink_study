package com.xjn.apitest.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author shkstart
 * @create 2021-11-04 11:21
 */
public class TableAPI02_CommonApi {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //这样创建表环境1.10默认用老版本planner，1.12默认用blink
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1 基于老版本planner的流处理(和上面结果相同)
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        //1.2 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        //1.3 基于新版本blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        //1.4 基于blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        //2. 表的创建：连接外部系统，读取数据
        //2.1 读取文件
        String path = "D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        Table table = tableEnv.from("inputTable");
//        table.printSchema();
//        tableEnv.toAppendStream(table, Row.class).print();

        //table API 简单转换 查询转换
        Table resultTable = table.select("id,temp").filter("id = 'sensor_6'");

        //聚合统计
        Table aggTable = table.groupBy("id").select("id,id.count as cnt,temp.avg as avgTemp");

        //使用Flink SQL实现
        Table sqlResultTable = tableEnv.sqlQuery("select id,temp from inputTable where id = 'sensor_6' ");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");

        tableEnv.toAppendStream(resultTable,Row.class).print("resultTable");
        //聚合操作不能使用toAPPendStream将表转换为流，需要用toRetractStream
        //新来一个数据，它会撤回前一次的结果，然后更新新的结果
        tableEnv.toRetractStream(aggTable,Row.class).print("aggtable");
        tableEnv.toAppendStream(sqlResultTable,Row.class).print("sqlResultTable");
        tableEnv.toRetractStream(sqlAggTable,Row.class).print("sqlAggTable");

        env.execute();


    }
}
