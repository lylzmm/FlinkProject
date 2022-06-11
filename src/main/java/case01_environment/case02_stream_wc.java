package case01_environment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class case02_stream_wc {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> streamSource = env.socketTextStream("192.168.10.102", 9999);

//        streamSource.flatMap()

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamSource.flatMap(new FlatMap()).keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.f0);


        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce(new Reduce());

        reduce.print("reduce");
        env.execute("Flink socket");
    }


    public static class Reduce implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
            return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
        }
    }

    public static class FlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] cols = s.split(" ");

            for (String col : cols) {
                collector.collect(new Tuple2<>(col, 1));
            }
        }
    }
}
