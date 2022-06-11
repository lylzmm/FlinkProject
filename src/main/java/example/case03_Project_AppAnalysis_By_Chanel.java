package example;

import bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * 页面广告点击量实时统计
 */
public class case03_Project_AppAnalysis_By_Chanel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new AppMarketingDataSource())
                .flatMap(new FlatMapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(MarketingUserBehavior mk, Collector<Tuple2<String, Long>> collector) throws Exception {
                        collector.collect(new Tuple2<>(mk.getChannel() + "_" + mk.getBehavior(), 1L));
                    }
                })
                .keyBy(key -> key.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Integer>>() {
                    final HashMap<String, Integer> map = new HashMap<String, Integer>();

                    @Override
                    public void processElement(Tuple2<String, Long> tuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        Integer count = map.getOrDefault(tuple2.f0, 0);
                        count++;
                        collector.collect(new Tuple2<>(tuple2.f0, count));
                        map.put(tuple2.f0, count);
                    }
                })
                .print();

        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
