package practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class case04_ReduceWordCount {
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
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> lastTp, Tuple2<String, Integer> tp) throws Exception {
                        System.out.println("lastTp" + lastTp);
                        System.out.println("tp" + tp);
                        return new Tuple2<String, Integer>(lastTp.f0, lastTp.f1 + tp.f1);
                    }
                })
                .print();

        env.execute();
    }
}
