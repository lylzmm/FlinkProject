package case03_transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class case08_connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 流式数据方式一数据
        DataStreamSource<String> ds1 = env.socketTextStream("192.168.10.102", 8888);

        // 流式数据方式二数据
        DataStreamSource<String> ds2 = env.socketTextStream("192.168.10.102", 9999);

        // 修改流式方式二的数据类型
        SingleOutputStreamOperator<Integer> map = ds2.map(String::length);

        // 两个流connect
        ConnectedStreams<String, Integer> connect = ds1.connect(map);

        connect.map()
        // 启动程序
        env.execute("case08_connect");
    }
}
