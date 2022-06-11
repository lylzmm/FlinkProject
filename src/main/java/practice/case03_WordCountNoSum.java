package practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class case03_WordCountNoSum {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("192.168.10.102", 9999);

        source
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String s1 : s.split(" ")) {
                            collector.collect(new Tuple2<>(s1, 1));
                        }
                    }
                })
                .keyBy(key -> key.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    final HashMap<String, Integer> map = new HashMap<>();

                    @Override
                    public void processElement(Tuple2<String, Integer> tp, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Integer count = map.getOrDefault(tp.f0, 0);
                        count++;
                        map.put(tp.f0, count);
                        collector.collect(new Tuple2<>(tp.f0, count));
                    }
                })
                .print();


        env.execute();
    }
}
