package case02_source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class case02_kafka {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.102:9092");
        props.put("group.id", "metric-group");

        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> metric = evn.addSource(new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), props));

        metric.print();
        evn.execute("Kafka Source");
    }
}
