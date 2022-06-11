package case08_window_api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * ReduceFunction
 */
public class case01_ReduceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = evn.socketTextStream("192.168.1.102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                return Tuple2.of(line, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);

        keyBy.print("keyBy ");

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyBy.timeWindow(Time.seconds(10));

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = timeWindow.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });

        reduce.print("reduce");

        evn.execute();
    }
}
