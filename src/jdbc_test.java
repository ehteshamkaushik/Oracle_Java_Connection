/**
 * Created by Kaushik on 7/2/2017.
 */

import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.sql.*;
public class jdbc_test extends DefaultHandler {
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
    static final String DB_URL = "jdbc:oracle:thin:@localhost:1521:orcl";

    //  Database credentials
    static final String USER = "hr";
    static final String PASS = "hr";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        long starttime = System.currentTimeMillis();
        try{
            //STEP 2: Register JDBC driver
            Class.forName("oracle.jdbc.driver.OracleDriver");

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            conn.setAutoCommit(false);

            //STEP 4: Execute a query
            System.out.println("Creating statement...");
            stmt = conn.createStatement();
            String sql;
            sql = "CREATE TABLE POSTS ( ID NUMBER NOT NULL , " +
                    "POSTTYPEID NUMBER, " +
                    "PARENTID NUMBER ," +
                    "ACCEPTEDANSWERID NUMBER ," +
                    "CREATIONDATE TIMESTAMP ," +
                    "SCORE NUMBER, " +
                    "VIEWCOUNT NUMBER , " +
                    "BODY VARCHAR(4000), " +
                    "OWNERUSERID NUMBER ," +
                    "LASTEDITUSERID VARCHAR2(100), " +
                    "LASTEDITORDISPLAYNAME VARCHAR2(200), " +
                    "LASTEDITDATE VARCHAR2(100), " +
                    "LASTACTIVITYDATE VARCHAR2(100), " +
                    "COMMUNITYOWNEDDATE VARCHAR2(100), " +
                    "CLOSEDDATE VARCHAR2(100), " +
                    "TITLE TEXT, " +
                    "TAGS TEXT, " +
                    "ANSWERCOUNT NUMBER , " +
                    "COMMENTCOUNT NUMBER ," +
                    "FAVOURITECOUNT NUMBER " +
                    ")";
            stmt.executeUpdate(sql);

            File file = new File("E:\\PostLinks.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();
            UserHandler userhandler = new UserHandler(conn,stmt);
            saxParser.parse(file, userhandler);


            stmt.close();
            conn.commit();
            conn.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        long endtime = System.currentTimeMillis();
        long duration = endtime-starttime;
        System.out.println("Time taken : " + duration + " millisec");
        System.out.println("Goodbye!");
    }
}
