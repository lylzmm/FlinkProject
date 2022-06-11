package case02_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class case01_test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.addSource(new MySource());
        stringDataStreamSource.print();

        env.execute("My Source");
    }

    public static class MySource implements SourceFunction<String> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (isRunning) {
                sourceContext.collect(String.valueOf(Math.floor(Math.random() * 100)));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
