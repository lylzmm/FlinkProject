package case03_transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class case09_union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> numDS1 = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> numDS2 = env.fromCollection(Arrays.asList(5, 6, 7, 8));
        DataStreamSource<Integer> numDS3 = env.fromCollection(Arrays.asList(9, 10));


        DataStream<Integer> unionDS = numDS1
                .union(numDS2)
                .union(numDS3);

        unionDS.print();
        env.execute();
    }
}
