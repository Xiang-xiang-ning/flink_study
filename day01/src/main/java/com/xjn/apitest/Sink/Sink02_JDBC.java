package com.xjn.apitest.Sink;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author shkstart
 * @create 2021-10-27 15:05
 */
public class Sink02_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> inputsource = env.readTextFile("D:\\IDEAworkspace\\flink_study\\day01\\src\\main\\resources\\sensor.txt");
        DataStream<SensorReading> mapDataStream = inputsource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });
        //自定义jdbc sink
        mapDataStream.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<SensorReading>{
        private Connection connection = null;
        PreparedStatement insertsql = null;
        PreparedStatement updatesql = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/flink_test","root","000000");
            insertsql = connection.prepareStatement("insert into sensor(id,temp) values(?,?)");
            updatesql = connection.prepareStatement("update sensor set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            insertsql.close();
            updatesql.close();
            connection.close();
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updatesql.setDouble(1,value.getTemperature());
            updatesql.setString(2,value.getId());
            updatesql.execute();
            if (updatesql.getUpdateCount()==0){
                insertsql.setString(1,value.getId());
                insertsql.setDouble(2,value.getTemperature());
                insertsql.execute();
            }
        }
    }
}
