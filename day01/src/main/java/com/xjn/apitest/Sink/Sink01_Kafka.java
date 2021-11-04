package com.xjn.apitest.Sink;

import com.xjn.apitestbean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Properties;

/**
 * @author shkstart
 * @create 2021-10-27 13:46
 */
public class Sink01_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> kafkaSource = env.addSource(
                new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
        DataStream<String> mapDataStream = kafkaSource.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2])).toString();
        });
        DataStreamSink<String> streamSink = mapDataStream.addSink(
                new FlinkKafkaProducer011<String>("hadoop102:9092", "sinktest", new SimpleStringSchema()));
        env.execute();
    }
}
