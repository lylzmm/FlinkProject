package case13_actual_combat;

import bean.AggSum;
import example.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;

public class Flink01_optimize_pv {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });


        env
                .readTextFile("example_data/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .assignTimestampsAndWatermarks(wms)  // 添加 Watermark
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv_" + new Random().nextInt(10), 1);
                    }
                }).keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))  // 分配窗口
                .sum(1)
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<>("pv", tuple2.f1);
                    }
                }).keyBy(tu -> tu.f0)
                .sum(1)
//                .aggregate(new myAgg(), new WindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        String timestamp = new Timestamp(window.getStart()).toString();
//                        Integer sum = input.iterator().next();
//
//                        out.collect(new Tuple2<>(timestamp, sum));
//
//                    }
//                })
//                .filter(st -> st.f0.contains("2017-11-26 18:00:00.0"))
//                .keyBy(tuple2 -> tuple2.f0)
//                .sum(1)
                .print()
        ;
        env.execute();
    }

    public static class myAgg implements AggregateFunction<Tuple2<String, Integer>, AggSum, Integer> {


        @Override
        public AggSum createAccumulator() {
            return new AggSum(0);
        }

        @Override
        public AggSum add(Tuple2<String, Integer> tuple2, AggSum aggSum) {
            return new AggSum(tuple2.f1 + aggSum.getSum());
        }

        @Override
        public Integer getResult(AggSum aggSum) {
            return aggSum.getSum();
        }

        @Override
        public AggSum merge(AggSum aggSum, AggSum acc1) {
            return new AggSum(aggSum.getSum() + acc1.getSum());
        }
    }
}

