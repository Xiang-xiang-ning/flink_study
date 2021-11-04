package com.xjn.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author shkstart
 * @create 2021-11-04 14:35
 */
public class TableAPI03_FileOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String path = "D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("timestamp",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");
        Table table = tableEnv.from("inputTable");
        Table resultTable = table.select("id,temp").filter("id = 'sensor_6'");
        Table aggTable = table.groupBy("id").select("id,id.count as cnt,temp.avg as avgTemp");
        Table sqlResultTable = tableEnv.sqlQuery("select id,temp from inputTable where id = 'sensor_6' ");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temp) as avgTemp from inputTable group by id");
        String outputPath = "D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\output.txt";
        //创建输出表和创建输入表方式一样
        //文件格式输出不支持聚合操作，因为文件系统写入的数据不能撤回
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("id",DataTypes.STRING())
                .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");
        resultTable.insertInto("outputTable");
        env.execute();
    }
}
