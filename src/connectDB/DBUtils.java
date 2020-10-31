package connectDB;

import java.sql.*;
import java.util.Date;


public class DBUtils {

    public static void main(String[] args){
        Connection connection = null;
        Connection connection1 = null;
        Statement statement = null;
        Statement statement1 =null;
//        String t = "报警号：500，报警时间：2018-6-9  10：55：48，报警内容：";
//        int i4 = t.lastIndexOf("：");
//        System.out.println(""+i4+"   "+t.length());
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/backup_58", "postgres", "123456" );
            connection1 = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/postgres", "postgres", "123456" );
            connection.setAutoCommit(false);
            System.out.println("数据库连接成功！");
            Date date = new Date();
            date.getTime();
            statement = connection.createStatement();
            statement1 = connection1.createStatement();
            statement.setFetchSize(500);

            int i=0;
            ResultSet resultSet = statement.executeQuery("select * from \"T_MDC_HISTORY_DATA_copy\" ");
            while (resultSet.next()){
                i += 1;
                String CNC_ID = resultSet.getString("CNC_ID");
                float MACH_XCOR = resultSet.getFloat("MACH_XCOR");
                float MACH_YCOR = resultSet.getFloat("MACH_YCOR");
                float MACH_ZCOR = resultSet.getFloat("MACH_ZCOR");
                float ABS_XCOR = resultSet.getFloat("ABS_XCOR");
                float ABS_YCOR = resultSet.getFloat("ABS_YCOR");
                float ABS_ZCOR = resultSet.getFloat("ABS_ZCOR");
                float RELV_XCOR = resultSet.getFloat("RELV_XCOR");
                float RELV_YCOR = resultSet.getFloat("RELV_YCOR");
                float RELV_ZCOR = resultSet.getFloat("RELV_ZCOR");
                float DIST_XCOR = resultSet.getFloat("DIST_XCOR");
                float DIST_YCOR = resultSet.getFloat("DIST_YCOR");
                float DIST_ZCOR = resultSet.getFloat("DIST_ZCOR");
                String POWER_STATUS = resultSet.getString("POWER_STATUS");
                String STOP_STATUS = resultSet.getString("STOP_STATUS");
                String RUN_STATUS = resultSet.getString("RUN_STATUS");
                float START_TIME = resultSet.getFloat("START_TIME");
                float WORK_ONCE_TIME = resultSet.getFloat("WORK_ONCE_TIME");
                float WORK_ALL_TIME = resultSet.getFloat("WORK_ALL_TIME");
                float WORK_NUM = resultSet.getFloat("WORK_NUM");
                float CUT_TIME = resultSet.getFloat("CUT_TIME");
                String PRG_NAME = resultSet.getString("PRG_NAME");
                String ALARM_MSG = resultSet.getString("ALARM_MSG");
                float FEED_SPEED = resultSet.getFloat("FEED_SPEED");
                float SPD_SPEED = resultSet.getFloat("SPD_SPEED");
                String ALARM_STATUS = resultSet.getString("ALARM_STATUS");
                float FEDSPD_RATE = resultSet.getFloat("FEDSPD_RATE");
                float SPIDSPD_RATE = resultSet.getFloat("SPIDSPD_RATE");
                float QUICK_MOVE_RATE = resultSet.getFloat("QUICK_MOVE_RATE");
                String SEQID = resultSet.getString("SEQID");
                Timestamp TIMEID = resultSet.getTimestamp("TIMEID");
                int i2 = 0;
                int i3 = 0;
                try {
                    i2 = ALARM_MSG.indexOf("，");
                    i3 = ALARM_MSG.lastIndexOf("：");
                }catch (Exception e){

                }
                System.out.println(i2);
                System.out.println(i3);
                String str1 = " ";
                String str2 = " ";
                String str3 = " ";
                if(i2 != 0 && i3 != 0 && i3>5){
                    str1 = ALARM_MSG.substring(4,i2);
                    str2 = ALARM_MSG.substring(i2+6,i3-5);
                }

                if(i3==0||i3==ALARM_MSG.length()-1) {

                }else {
                    str3 = ALARM_MSG.substring(i3+1);
                }
                String LAST_ALARM_TIME = str2;
                String LAST_ALARM_CONTEXT = str1+ ":"+str3;

                System.out.println("第"+i+"次查询："+":::::"+ALARM_MSG);
                String sql = "insert into \"T_MDC_HISTORY_DATA_Transform\"  (\"CNC_ID\",\"MACH_XCOR\",\"MACH_YCOR\",\"MACH_ZCOR\"," +
                        "\"ABS_XCOR\",\"ABS_YCOR\",\"ABS_ZCOR\",\"RELV_XCOR\",\"RELV_YCOR\",\"RELV_ZCOR\",\"DIST_XCOR\",\"DIST_YCOR\"," +
                        "\"DIST_ZCOR\",\"POWER_STATUS\",\"STOP_STATUS\",\"RUN_STATUS\",\"START_TIME\",\"WORK_ONCE_TIME\",\"WORK_ALL_TIME\"," +
                        "\"WORK_NUM\",\"CUT_TIME\",\"PRG_NAME\",\"ALARM_MSG\",\"FEED_SPEED\",\"SPD_SPEED\",\"ALARM_STATUS\",\"FEDSPD_RATE\"," +
                        "\"SPIDSPD_RATE\",\"QUICK_MOVE_RATE\",\"SEQID\",\"TIMEID\",\"LAST_ALARM_TIME\",\"LAST_ALARM_CONTEXT\")" +
                        "VALUES"+"('"+CNC_ID+"',"+
                        MACH_XCOR+","+MACH_YCOR+","+MACH_ZCOR+ ","+ABS_XCOR+","+ABS_YCOR+","+ABS_ZCOR+","+RELV_XCOR+","+
                        RELV_YCOR+","+ RELV_ZCOR+","+DIST_XCOR+","+DIST_YCOR+","+DIST_ZCOR+",'"+
                        POWER_STATUS+"','"+STOP_STATUS+"','"+RUN_STATUS+"',"+START_TIME+","+WORK_ONCE_TIME+","+
                        WORK_ALL_TIME+","+WORK_NUM+","+
                        CUT_TIME+",'"+PRG_NAME+"','"+ALARM_MSG+"',"+FEED_SPEED+","+SPD_SPEED+",'"+ALARM_STATUS+"',"+
                        FEDSPD_RATE+","+SPIDSPD_RATE+","+
                        QUICK_MOVE_RATE+",'"+SEQID+"','"+TIMEID+"','"+LAST_ALARM_TIME+"','"+LAST_ALARM_CONTEXT+"');";
                System.out.println(sql);
                statement1.executeUpdate(sql);
            }
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }
    public String adjust(String s){

        return s;
    }
}
