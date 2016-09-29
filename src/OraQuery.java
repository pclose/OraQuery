/*  OraQuery -pete 2016-09-20
 *
 * Usage: java OraQuery <db_host> <db_user> <db_password> [-i <sql_file>] [-o <output_file>]
 *      If -i or -o are omitted stdin and stdout will be substituted
 *
 * Program to export data in an Oracle database to CSV
 * Dependencies:
 *  - ojdbc6.jar from Oracle
 *  - opencsv-x.x.jar from opencsv.sourceforge.net
 */


import oracle.jdbc.pool.OracleDataSource;

import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.PrintStream;
import java.io.File;
import java.io.PrintWriter;
import java.util.Scanner;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.sql.Clob;

public class OraQuery {
    
    private Connection db_conn;
    private int db_column_count;
    private int[] db_column_type;
    
    private static String USAGE =
    "java OraQuery <db_host> <db_user> <db_password> [-i <sql_file>] [-o <output_file>]\n" +
    "\tIf -i or -o are omitted stdin and stdout will be substituted\n\n";
    
    public static void main(String[] args) {
    
        String db               = null;
        String pw               = null;
        String user             = null;
        PrintWriter output_fh   = null;
        Scanner input_fh        = null;
        
        // Print help and exit if -h or --help are in args
        for (String e : args) {
            if (e.matches("-h|--help")) {
                prUsage();
                System.exit(0);
            }
        }
        
        // lazy- just base parsing on number of arguments
        if (args.length < 3) {
            prUsage();
            System.exit(1);
        }
        
        db      = args[0];
        user    = args[1];
        pw      = args[2];
        
        // Filter and assign input and output file flags
        try {
            
            for (int i = 3; i < args.length; i++) {
                if (args[i].equals("-i") && (args.length > i)) {
                    input_fh = new Scanner(new File(args[i+1]));
                } else if (args[i].equals("-o") && args.length > i) {
                    output_fh = new PrintWriter(args[i+1]);
                }
            }
            
            // Default to stdin/stdout
            if (input_fh == null) {
                input_fh = new Scanner(System.in);
            }
            if (output_fh == null) {
                output_fh = new PrintWriter(System.out);
            }
            
        } catch (Exception ex) {
            prUsage();
            ex.printStackTrace(System.err);
            System.exit(1);
        }
        
        
        try {
            // Init constructor that will do the work
            OraQuery q = new OraQuery(db, user, pw, input_fh, output_fh);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            System.exit(1);
        }
        
        
    }
    
    
    
    /* OraQuery- private constructor call from main
     * does all the run logic */
    private OraQuery(String db, String user, String pw, Scanner input, PrintWriter output) throws SQLException, IOException {
        
        try {
        
            // Load up sql string
            StringBuffer sql = new StringBuffer("");
            while (input.hasNextLine()) {
                sql.append(input.nextLine() + "\n");
            }
            
            // Setup database connection
            setupConn(db, user, pw);
            
            // Create statement and run
            Statement stmt      = db_conn.createStatement();
                                  stmt.execute(sql.toString());
            ResultSet records   = stmt.getResultSet();
            int update_ct       = stmt.getUpdateCount();
            
            
            // If this query updated anything
            if (update_ct >= 0) {
                output.printf("Records updated: %d\n", update_ct);
                output.flush();
                output.close();
            
            // Otherwise loop through results and write to CSV
            } else {
                CSVWriter csv = new CSVWriter(output);
                csv.writeNext(procHeader(records.getMetaData()));
                while (records.next()) {
                    csv.writeNext(procRecord(records));
                }
                csv.close();
            }
        
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            db_conn.close();
            throw ex;
        }
        db_conn.close();
        
    }
    
    
    
    /* setupConn - sets up the database connection object
     * */
    private void setupConn(String db, String user, String pw) throws SQLException, IOException {
    
        OracleDataSource ods = new OracleDataSource();
        ods.setURL("jdbc:oracle:thin:@" + db);
        ods.setUser(user);
        ods.setPassword(pw);
        
        db_conn = ods.getConnection();
    }
    
    
    
    // Print usage info
    private static void prUsage() {
        System.out.print(USAGE);
    }
    
    
    
    // Returns an array of column names
    private String[] procHeader(ResultSetMetaData header) throws SQLException {
        db_column_count = header.getColumnCount();
        db_column_type = new int[db_column_count];
        String[] result = new String[db_column_count];
        for (int i = 0; i < db_column_count; i++) {
            result[i] = header.getColumnName(i+1);
            db_column_type[i] = header.getColumnType(i+1);
        }
        return result;
    }
    
    
    
    // Returns an array of Strings for a given row of data
    private String[] procRecord(ResultSet record) throws SQLException {
        String[] result = new String[db_column_count];
        for (int i = 0; i < db_column_count; i++) {
            
            Object temp = record.getObject(i+1);
            
            // null
            if (record.wasNull()) {
                result[i] = "";
            
            // CLOB
            } else if (db_column_type[i] == Types.CLOB) {
                Clob clob = (Clob) temp;
                result[i] = clob.getSubString(1, (int) clob.length());
            
            // TODO: handle each data type specifically
            } else {
                try {
                    result[i] = temp.toString();
                } catch (Exception ex) {
                    result[i] = "**OraQuery Failure**";
                }
            }
            
        }
        return result;
    }
    

}
