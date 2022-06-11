package case02_source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class case03_mysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = evn.addSource(new SourceFromMySQL());

        source.print();

        evn.execute("Source Mysql");
    }

    public static class SourceFromMySQL extends RichSourceFunction<String> {

        PreparedStatement ps;
        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "select * from temp;";
            ps = this.connection.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            ResultSet resultSet = ps.executeQuery();
            while (true) {
                while (resultSet.next()) {

                    int id = resultSet.getInt("id");
                    String date = resultSet.getString("name");
                    String uid = resultSet.getString("time").trim();
                    int keyword = resultSet.getInt("type");
                    ctx.collect(date);
                }
            }

        }

        @Override
        public void cancel() {

        }

        private static Connection getConnection() {
            Connection con = null;
            try {
                Class.forName("com.mysql.jdbc.Driver");
                con = DriverManager.getConnection("jdbc:mysql://192.168.1.102:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "000000");
            } catch (Exception e) {
                System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
            }
            return con;
        }
    }
}
