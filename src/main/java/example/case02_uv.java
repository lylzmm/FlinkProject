package example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;


/**
 * 网站总浏览量（PV）的统计
 */
public class case02_uv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .readTextFile("example_data/UserBehavior.csv")
                .map(data -> {
                    String[] datas = data.split(",");
                    return new UserBehavior(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            Integer.valueOf(datas[2]),
                            datas[3],
                            Long.valueOf(datas[4])
                    );
                })
                .flatMap(new FlatMapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(UserBehavior userBehavior, Collector<Tuple2<String, Long>> collector) throws Exception {
                        if ("pv".equals(userBehavior.getBehavior())) {
                            Tuple2<String, Long> uv = Tuple2.of("uv", userBehavior.getUserId());
                            collector.collect(uv);
                        }
                    }
                })
                .keyBy(key -> key.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    final HashSet<Long> set = new HashSet<Long>();

                    @Override
                    public void processElement(Tuple2<String, Long> stringLongTuple2, Context context, Collector<Integer> collector) throws Exception {
                        set.add(stringLongTuple2.f1);

                        collector.collect(set.size());
                    }
                })
                .print();

        env.execute();

    }
}
