package case01_environment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class case01_wc {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> textFile = env.readTextFile("input/WordCount");
        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(new Tuple2<String, Integer>(s1, 1));
                }
            }
        });

        System.out.println(stringTuple2FlatMapOperator);
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = stringTuple2FlatMapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);
        sum.print();
    }
}
