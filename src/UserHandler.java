

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by Kaushik on 7/5/2017.
 */
public class UserHandler extends DefaultHandler {
    int noOfRows=0;
    String userid, id, postid, relatedpostid, postlinktypeid;
    String name, time;
    Connection c = null;
    Statement stmt = null;

    public UserHandler(Connection connection,Statement statement)
    {
        c=connection;
        stmt=statement;
    }

    @Override
    public void startElement(String uri,
                             String localName, String qName, Attributes attributes)
            throws SAXException {
        if (qName.equalsIgnoreCase("row")) {

            id = attributes.getValue("Id");
            postid = attributes.getValue("PostId");
            relatedpostid = attributes.getValue("RelatedPostId");
            postlinktypeid = attributes.getValue("PostLinkTypeId");
            time = attributes.getValue("CreationDate");
            String[] parts = time.split("T");
            String part1 = parts[0]; // 004
            String part2 = parts[1]; // 034556
            String date = "to_timestamp('" + part1 + " " + part2 + "', 'yyyy-mm-dd hh24:mi:ss.ff')";

            try {
                String sql = "INSERT INTO POSTLINKS (ID, CREATIONDATE, POSTID, RELATEDPOSTID, POSTLINKTYPEID)" +
                        " VALUES ( "+ id + ", " + date + ", " + postid +", " + relatedpostid + ", " + postlinktypeid + ")";
                stmt.executeUpdate(sql);


            } catch (SQLException e) {
                e.printStackTrace();
            }
            noOfRows++;
            System.out.println(noOfRows);
        }
    }

    @Override
    public void endDocument()
    {
        System.out.println(noOfRows);
    }
}
