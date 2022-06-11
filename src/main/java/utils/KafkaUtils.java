//package utils;
//
//import case02_source.Student;
//import com.alibaba.fastjson.JSON;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//
//import java.util.Properties;
//
//public class KafkaUtils {
//
//    private static final String broker_list = "192.168.1.102:9092";
//    private static final String topic = "metric";  // kafka topic，FLink 程序中需要和这个统一
//
//
//    public static void main(String[] args) {
//        writeToKafka();
//    }
//
//
//    private static void writeToKafka() {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", broker_list);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//
//        for (int i = 1; i <= 10; i++) {
//            Student student = new Student(i, "zhisheng" + i, "password" + i, 18 + i);
//            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, null, JSON.toJSONString(student));
//            producer.send(record);
//            System.out.println("发送数据: " + JSON.toJSONString(student));
//        }
//        producer.flush();
//    }
//}
