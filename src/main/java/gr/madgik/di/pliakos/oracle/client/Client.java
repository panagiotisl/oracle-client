package gr.madgik.di.pliakos.oracle.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * The Class Client is an executable class registers a JDBC connection with an
 * Oracle database, submits a query to the database and outputs the results to
 * stdout.
 */
public class Client {

    /** The default connection string. */
    static final String LOCAL = "jdbc:oracle:thin:@localhost:1521:xe";

    /** The default username. */
    static final String LOCAL_USER = "system";

    /** The default password. */
    static final String LOCAL_PASSWD = "oracle";

    /** A logger. */
    final static Logger LOG = Logger.getLogger(Client.class);

    /** The query should be given as a command line parameter. */
    @Parameter(names = { "--query",
            "-q" }, description = "the query to be executed (should be enclosed with single quotes)", required = true)
    public String query;

    /** The connection string. */
    @Parameter(names = { "--oracle-conection-string", "-o" }, description = "the oracle connection string")
    public String connectionString = LOCAL;

    /** The username. */
    @Parameter(names = { "--username", "-u" }, description = "the username")
    public String username = LOCAL_USER;

    /** The password. */
    @Parameter(names = { "--password", "-p" }, description = "the password")
    public String password = LOCAL_PASSWD;

    /** The help parameter outputs a usage description. */
    @Parameter(names = { "--help", "-h" }, help = true, description = "Usage description")
    private boolean help = false;

    /**
     * The main method registers a JDBC connection with an Oracle database,
     * submits a query to the database and outputs the results to stdout.
     *
     * @param args
     *            the arguments
     */
    public static void main(String[] args) {

        Client client = new Client();

        JCommander.newBuilder().addObject(client).build().parse(args);

        if (client.help) {
            JCommander.newBuilder().addObject(client).build().usage();
            return;
        }

        LOG.debug("-------- Oracle JDBC Connection Testing ------");

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            LOG.error("Where is your Oracle JDBC Driver?", e);
            return;

        }

        LOG.debug("Oracle JDBC Driver Registered!");

        Connection connection = null;

        try {

            connection = DriverManager.getConnection(client.connectionString, client.username, client.password);

        } catch (SQLException e) {

            LOG.error("Connection Failed! Check output console", e);
            return;

        }

        if (connection != null) {
            LOG.debug("You made it, take control your database now!");
            try {
                Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery(client.query);
                ResultSetMetaData metadata = rs.getMetaData();
                int columnCount = metadata.getColumnCount();
                String columns = "";
                for (int i = 1; i <= columnCount; i++) {
                    columns += metadata.getColumnName(i) + ",";
                }
                System.out.println(columns.substring(0, columns.length() - 1));
                while (rs.next()) {
                    String row = "";
                    for (int i = 1; i <= columnCount; i++) {
                        row += rs.getString(i) + ",";
                    }
                    System.out.println(row.substring(0, row.length() - 1));
                }
            } catch (SQLException ex) {
                LOG.error(ex.getMessage(), ex);
            }

        } else {
            LOG.error("Failed to make connection!");
        }
    }

}
