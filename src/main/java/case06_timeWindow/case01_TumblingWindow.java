package case06_timeWindow;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;

public class case01_TumblingWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.10.102", 9999);

        socketTextStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String s : line.split(" ")) {
                            collector.collect(new Tuple2<String, Integer>(s, 1));
                        }
                    }
                })
                .keyBy(key -> key.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1)

                // =========================================== 累加聚合又能拿到窗口时间
//                .aggregate(new myaAggregate(), new WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>() {
//                    @Override
//                    public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Integer count = input.iterator().next();
//                        System.out.println("=============" + input);
//
//                        String timestamp = new Timestamp(window.getStart()).toString();
//                        out.collect(new Tuple2<>(new Timestamp(window.getStart()) + key, count));
//                    }
//                })

                // =========================================== 全量窗口
//                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
//                    @Override
//                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        ArrayList<Tuple2<String, Integer>> tuple2s = Lists.newArrayList(elements.iterator());
//                        out.collect(new Tuple2<>(key, tuple2s.size()));
//                    }
//                })

//                // =========================================== 全量窗口
//                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
//                    @Override
//                    public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        ArrayList<Tuple2<String, Integer>> tuple2s = Lists.newArrayList(input.iterator());
//                        out.collect(new Tuple2<>(key, tuple2s.size()));
//                    }
//                })
                .print();


        env.execute();
    }

    public static class myaAggregate implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return integer + acc1;
        }
    }


}
