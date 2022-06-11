package practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;

public class case02_Distinct {
    public static void main(String[] args) throws Exception {
        HashSet<String> set = new HashSet<>();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.10.102", 9999);

        source
                .keyBy(key -> key)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        if (set.contains(s)) return false;
                        else {
                            set.add(s);
                            return true;
                        }

                    }
                })
                .print();


        env.execute();
    }
}
