//package case02_source;
//
//import org.apache.flink.api.common.io.FileOutputFormat;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.typeutils.RowTypeInfo;
//import org.apache.flink.types.Row;
//
//public class Test {
//    public static void main(String[] args) throws Exception {
//        String driverClass = "com.mysql.jdbc.Driver";
//        String dbUrl = "jdbc:mysql://192.168.1.102:3306/test";
//        String userName = "root";
//        String passWord = "000000";
//
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        //读mysql
//        DataSource<Row> dataSource = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
//                .setDrivername(driverClass)
//                .setDBUrl(dbUrl)
//                .setUsername(userName)
//                .setPassword(passWord)
//                .setQuery("select name,time from temp")
//                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
//                .finish());
//
//        dataSource.print();
//    }
//}
