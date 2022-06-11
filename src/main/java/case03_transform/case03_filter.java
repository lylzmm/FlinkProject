package case03_transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;

public class case03_filter {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> readTextFile = env.readTextFile("input/WordCount");
        FilterOperator<String> filter = readTextFile.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.contains("hello");
            }
        });

        filter.print();
    }
}
