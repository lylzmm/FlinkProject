package case03_transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

public class case02_flatMap {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment evn = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> readTextFile = evn.readTextFile("input/WordCount");

        FlatMapOperator<String, String> flatMap = readTextFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {

                String[] split = line.split(" ");
                for (String s : split) {
                    collector.collect(s);
                }
            }
        });
        flatMap.print();
    }

}
