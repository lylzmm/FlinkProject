package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 网站总浏览量（PV）的统计
 */
public class case01_pv {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("example_data/UserBehavior.csv")
                .map(data -> {
                    String[] datas = data.split(",");
                    return new UserBehavior
                            (
                                    Long.valueOf(datas[0]),
                                    Long.valueOf(datas[1]),
                                    Integer.valueOf(datas[2]),
                                    datas[3],
                                    Long.valueOf(datas[4])
                            );
                })
                .flatMap(new FlatMapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(UserBehavior userBehavior, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        if (userBehavior.getBehavior().contains("pv")) {
                            collector.collect(Tuple2.of("pv", 1));
                        }
                    }
                })
                .keyBy(behavior -> behavior.f0)
                .sum(1)
                .print();


        env.execute();
    }
}
