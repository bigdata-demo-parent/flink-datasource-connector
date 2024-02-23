package com.xiaoxiaomo.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author tang.xuandong
 * @version 1.0.0
 * @date 2024/2/23 11:03
 */
public class KafkaConnectorOldDemo {

    static String brokers = "192.168.13.36:9092,192.168.13.36:9092,192.168.13.36:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("group.id", "test-group");
        // Consumer
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "test",
                new SimpleStringSchema(),
                properties
        );

        flinkKafkaConsumer.setStartFromEarliest();

        DataStream<String> dataStream = env.addSource(flinkKafkaConsumer);

        // 分割成单词，统计数量
        dataStream
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute("Connector DataSource old demo : kafka");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
