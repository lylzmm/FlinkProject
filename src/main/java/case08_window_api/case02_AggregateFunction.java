package case08_window_api;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * AggregateFunction
 */
public class case02_AggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.1.102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> aggregate = keyBy.timeWindow(Time.seconds(10)).aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return Tuple2.of(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(Tuple2<String, Integer> t1, Tuple2<Integer, Integer> t2) {
                return Tuple2.of(t1.f1 + t2.f0, t2.f1 + 1);
            }

            @Override
            public Tuple2<String, Integer> getResult(Tuple2<Integer, Integer> t1) {
                return Tuple2.of("sensor", (t1.f0 / t1.f1));
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
                return Tuple2.of(t1.f0 + t2.f0, t1.f1 + t2.f1);
            }
        });

        aggregate.print("aggregate");

        env.execute();
    }
}
