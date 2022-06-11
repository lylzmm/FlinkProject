package case03_transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.Environment;

public class case01_map {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment evn = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> readTextFile = evn.readTextFile("input/WordCount");
        MapOperator<String, Tuple2<String, Integer>> map = readTextFile.map((MapFunction<String, Tuple2<String, Integer>>) s -> new Tuple2<>(s, 1));


        map.print();
    }
}
