package com.xjn.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;

import java.util.Properties;

/**
 * @author shkstart
 * @create 2021-10-26 15:22
 */
public class SourceTest03_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        //Kafka与flink连接的数据流
        DataStreamSource<String> datasource = env.addSource(
                new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties)
        );
        datasource.print();
        env.execute();
    }
}
