import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.IOException;
import java.util.Properties;

import java.time.LocalDateTime;
import java.sql.Timestamp;
import java.util.Vector;
import java.util.concurrent.Callable;

public class GigSystem {

    /* [Option 1 Gig Line-Up]: This option is to find the act, its ontime, and its offtime given by a gig ID. */
    public static String[][] option1(Connection conn, int gigID){
        try{
            maintainCheck(conn);

            String selectActs = "SELECT actname, ontime, offtime FROM option1 WHERE gigID = ? ORDER BY ontime";
            PreparedStatement searchActs = conn.prepareStatement(selectActs);
            
            // SQL query: SELECT actname, ontime, offtime FROM option1 WHERE gigID = gig_id ORDER BY ontime.
            searchActs.setInt(1, gigID);
            ResultSet getActs = searchActs.executeQuery();
            
            // SQL result: Act Name, on Time, off Time.
            String acts[][] = convertResultToStrings(getActs);

            maintainCheck(conn);
            searchActs.close();
            getActs.close();
            return acts;
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /* [Option 2: Organising a Gig]: This option is to create a new gig and then check whether it follows the criteria (rollback if not).*/
    public static void option2(Connection conn, String venue, String gigTitle, int[] actIDs, int[] fees, LocalDateTime[] onTimes, int[] durations, int adultTicketPrice){
        try{
            maintainCheck(conn);

            // Turn off auto commit to roll back if criteria of inserted gig is found improper.
            conn.setAutoCommit(false);

            // Create savepoint in the beginning so that the potential rollback can go back to the beginning state.
            Savepoint savepoint = conn.setSavepoint();

            // Set up gig first with venue, gig title, and gig date.
            // SQL parameter: FUNCTION setupGig(venue_name VARCHAR(100), gig_title VARCHAR(100), on_time TIMESTAMP).
            PreparedStatement setupGig = conn.prepareStatement("SELECT setupGig(?, ?, ?)");
            setupGig.setString(1, venue);
            setupGig.setString(2, gigTitle);
            // "Timestamp.valueOf(LocalDateTimes)" changes a LocalDateTimes type data into a Timestamp type data.
            // According to instruction, gigdate is set to onTimes[0].
            setupGig.setTimestamp(3, Timestamp.valueOf(onTimes[0]));
            // SQL result: gigID.
            ResultSet getGigID = setupGig.executeQuery();
            // Retrieve the gigID (serial) along with setting up a gig.
            int gigID = 0;
            while(getGigID.next()){gigID = getGigID.getInt(1);}

            // Set up gig_ticket second.
            PreparedStatement insertGigTicket = conn.prepareStatement("CALL insertGigTicket(?, ?, ?)");
            insertGigTicket.setInt(1, gigID);
            // The inserted ticket pricetype is "A" (Adult ticket).
            insertGigTicket.setString(2, "A");
            insertGigTicket.setInt(3, adultTicketPrice);
            // No need to retrieve any data so "execute" is sufficient.
            insertGigTicket.execute();

            // Set up act_gig at last.
            // SQL parameter: PROCEDURE insertActGig(act_id INTEGER, gig_id INTEGER, act_fee INTEGER, on_time TIMESTAMP, act_duration INTEGER).
            PreparedStatement insertActGig = conn.prepareStatement("CALL insertActGig(?, ?, ?, ?, ?)");
            // This for loop loops over all act's performance information.
            for(int x = 0; x < actIDs.length; x++){
                insertActGig.setInt(1, actIDs[x]);
                insertActGig.setInt(2, gigID);
                insertActGig.setInt(3, fees[x]);
                // "Timestamp.valueOf(LocalDateTimes)" changes a LocalDateTimes type data into a Timestamp type data.
                insertActGig.setTimestamp(4, Timestamp.valueOf(onTimes[x]));
                insertActGig.setInt(5, durations[x]);
                insertActGig.execute();
            }

            /* Why having setupGig, insertGigTicket, and insertActGig as 3 seperate parts:
            (1) Encapsulation: creating seperate functions or procedures in schema.sql improve its modularity.
            (2) Runtime: gig's and gig ticket's information needs inserting only once, but act's performance needs multiple.
            */
            
            /* Check whether the inserted gig follow the criteria including:
            (1) TIME CONFLICT: act's performance overlaps or act starts before the gig date.
            (2) TIME INTERVAL TOO LONG: act's performance gap is larger than 20 minutes or the first act starts 20 minutes later than the gig date.
            (3) ACT OVERTIME: an act plays longer than 2 hours.
            (4) DATE CROSSED: acts in a given gig plays on different date (which means crossing the midnight).
            (5) VENUE OVERLOAD: the ticket sold is greater than the venue capacity.

            Return [TRUE] if criteria is not followed, and therefore the database rollbacks to the initial save point.
            For details, please turn to FUNCTION checkCriteria(gig_id INTEGER) in schema.sql;
            */

            // Checking process.
            PreparedStatement checkCriteria = conn.prepareStatement("SELECT * FROM checkCriteria(?)");
            checkCriteria.setInt(1, gigID);
            ResultSet getGigStatus = checkCriteria.executeQuery();

            // Retrieve gigStatus by checking criteria of current gig.
            boolean gigStatus = false;
            while(getGigStatus.next()){gigStatus = getGigStatus.getBoolean(1);}
            
            // gigStatus is true if criteria is break; return false otherwise.
            if(gigStatus){
                conn.rollback(savepoint);
            }
            
            // Commit to proceed and set AutoCommit back to TRUE;
            conn.commit();
            conn.setAutoCommit(true);

            setupGig.close();
            getGigID.close();
            insertGigTicket.close();
            insertActGig.close();
            checkCriteria.close();
            getGigStatus.close();

            maintainCheck(conn);

        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
    }

    /* [Option 3: Booking a Ticket]: This option is to insert a new ticket information into TABLE ticket.*/
    public static void option3(Connection conn, int gigid, String name, String email, String ticketType){
        try{
            maintainCheck(conn);

            // SQL parameter: PROCEDURE insertTicket(gig_id INTEGER, price_type VARCHAR(2), customer_name VARCHAR(100), customer_email VARCHAR(100)).
            /* There are some criteria needs checking before purchase: 
            (1)GIG NOT FOUND: gig with a given ID is not found.
            (2)PRICETYPE NOT FOUND: gig ticket's pricetype is not found.  
            (3)NO AVAILABLE SEAT: the current amount of ticket sold is greater or equal to the venue capacity.
            Please turn to PROCEDURE insertTicket in schema.sql for details. 
            */
            PreparedStatement insertTicket = conn.prepareStatement("CALL insertTicket(?, ?, ?, ?)");
            insertTicket.setInt(1, gigid);
            insertTicket.setString(2, ticketType);
            insertTicket.setString(3, name);
            insertTicket.setString(4, email);

            insertTicket.execute();
            insertTicket.close();

            maintainCheck(conn);

        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
    }

    /* [Option 4: Cancelling an Act]: This option is to cancal a certain act from a specified gig
        and then check whether it follows the criteria (cancal the entire gig if not).*/
    public static String[] option4(Connection conn, int gigID, String actName){
        try{
            maintainCheck(conn);

            // SQL parameter: removeActfromGig(gig_id INTEGER, act_name VARCHAR(100)
            PreparedStatement removeActfromGig = conn.prepareStatement("SELECT * FROM removeActfromGig(?, ?)");
            removeActfromGig.setInt(1, gigID);
            removeActfromGig.setString(2, actName);

            /* Check whether the inserted gig follow the criteria including:
            (1) TIME CONFLICT: act's performance overlaps or act starts before the gig date.
            (2) TIME INTERVAL TOO LONG: act's performance gap is larger than 20 minutes or the first act starts 20 minutes later than the gig date.
            (3) ACT OVERTIME: an act plays longer than 2 hours.
            (4) DATE CROSSED: acts in a given gig plays on different date (which means crossing the midnight).
            (5) VENUE OVERLOAD: the ticket sold is greater than the venue capacity.

            Return [TRUE] if criteria is not followed, and therefore the database rollbacks to the initial save point.
            For details, please turn to FUNCTION checkCriteria(gig_id INTEGER) in schema.sql;
            */

            // Retrieve gigStatus by checking criteria of current gig.
            Boolean gigStatus = false;
            ResultSet getGigStatus = removeActfromGig.executeQuery();
            while(getGigStatus.next()){gigStatus = getGigStatus.getBoolean(1);}

            removeActfromGig.close();
            getGigStatus.close();

            // gigStatus is true if criteria is break; return false otherwise.
            if(gigStatus){
                // SQL parameter: PROCEDURE setAffectedTicket(gig_id INTEGER).
                PreparedStatement setAffectedTicket = conn.prepareStatement("CALL setAffectedTicket(?)");
                setAffectedTicket.setInt(1, gigID);
                setAffectedTicket.execute();
                setAffectedTicket.close();

                // Get the amount of affected customers.
                // VIEW VIEW_setaffectedTicket includes all customer's email of a gig. If name needed, include customer's name in the view.
                PreparedStatement searchAmount = conn.prepareStatement("SELECT COUNT(*) FROM VIEW_setAffectedTicket");
                ResultSet getAmount = searchAmount.executeQuery();
                int amount = 0;
                while(getAmount.next()){amount = getAmount.getInt(1);}
                searchAmount.close();
                getAmount.close();
                
                // Initialization of ARRAY email and INTEGER counter for later storing.
                int counter = 0;
                String email[] = new String[amount];

                // Get the email list and then transfer it into Array email.
                // VIEW VIEW_setaffectedTicket includes all customer's email of a gig. If name needed, include customer's name in the view.
                PreparedStatement searchEmail = conn.prepareStatement("SELECT * FROM VIEW_setAffectedTicket");

                // SQL result: email.
                ResultSet getEmail = searchEmail.executeQuery();
                while(getEmail.next()){
                    email[counter] = getEmail.getString(1);
                    counter ++;
                }
                searchEmail.close();
                getEmail.close();
                maintainCheck(conn);
                return email;
            }
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        maintainCheck(conn);
        return new String[0];
    }

    /* [Option 5: Tickets Needed to Sell]: This option show how many "A"(adult) tickets need selling to reimburse the expense. */
    public static String[][] option5(Connection conn){
        try{
            maintainCheck(conn);

            /* VIEW ticketToSell includes:
            (1) gigID: gig serial ID.
            (1) cost_to_reimburse: this includes all actfees (act agreed fee) and venue hirecost.
            (2) adult_ticket_price: this retrieves the ticket price from gig_ticket using gig ID.
            (3) adult_ticket_to_sell: the amount of adult tickets need selling to cover cost_to_reimburse.
                Notice that the ticket must be an integer and therefore if this amount has decimal places, a ceiling is required.
            There are some intermediate views used during this process, e.g. totalActFee, venueFee, and balance.
            
            Notice that if a gig has no pricetype 'A', it will still show on the views like balance, but not on the VIEW ticketToSell.
            Please turn to VIEW ticketToSell in schema.sql for details.
            */
            PreparedStatement searchTicketToSell = conn.prepareStatement("SELECT gigid, adult_ticket_to_sell FROM ticketToSell");
            ResultSet getTicketToSell = searchTicketToSell.executeQuery();

            // SQL result: gigID, adult_ticket_to_sell.
            String ticketToSell[][] = convertResultToStrings(getTicketToSell);
            searchTicketToSell.close();
            getTicketToSell.close();
            maintainCheck(conn);
            return ticketToSell;
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /* [Option 6: How Many Tickets Sold]: This option shows the amount of tickets an act sold as a headline. */
    public static String[][] option6(Connection conn){
        try{
            maintainCheck(conn);

            /* VIEW actUnionTicket includes:
            (1) actname: name of the act which has played as a headline.
            (2) year: the year which the act has played as a headline (for at least once) along with a "Total".
            (3) year_ticket_sold: the amount of tickets sold for the gigs as the act is a headline.
            There are some intermediate views used during this process, e.g. headlinetime, gigheadline, ticketSold.
            
            Notice that according to the requirement, this view first put the same actname together in a "block", 
                and the "block" inside is in an order of year with "Total" at last.
                In short, it is achieved by ranking the total amount of tickets sold by a headline act first,
                and then have the rank inherited(copied) by the row which records the yearly amount of tickets sold by
                the same act. Therefore, the row with a same actname will stay in a "block". Within the block, it is 
                ranked by "Year" column in an order of smaller year - larger year - "Total" (comparable as they are all text).
            Please turn to VIEW actUnionTicket in schema.sql for details.
            */
            PreparedStatement searchActTicket = conn.prepareStatement("SELECT * FROM actUnionTicket");
            ResultSet getActTicket = searchActTicket.executeQuery();

            // SQL result: actname, year, year_ticket_sold.
            String actTicket[][] = convertResultToStrings(getActTicket);
            searchActTicket.close();
            getActTicket.close();
            maintainCheck(conn);
            return actTicket;
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /* [Option 7 Regular Customers]: This option shows a list of regular customers of acts. 
        Regular customer (RC) definition: a customer of the act who buys the ticket of this act being a headline for at least 
        once every "year" (as the "year" means that an act used to be a headline in this year).*/
    public static String[][] option7(Connection conn){
        try{
            maintainCheck(conn);

            // SQL parameter: PROCEDURE getAllRC().
            PreparedStatement checkRC = conn.prepareStatement("CALL getAllRC()");
            checkRC.execute();

            /* VIEW preparedRC includes:
            (1) actname: name of the act which used to be a headline.
            (2) customername: name of RC.
            There are some intermediate views used during this process, please turn to VIEW preparedRC in shema.sql for details.

            To get (1) actname in VIEW preparedRC, first we form a view which includes the gigID, the name of headline (by obtaining
                the largest ontime and matching the act), and the year of performance. Based on this view, we create another view 
                with distinct year, which lists the years of an act used to be a headline (as RC is required to buy a ticket from 
                at least one ticket for each).
            To get (2) customername, based on the actname and year we derived from the last view, we check whether a customer buy at 
                least one ticket for each year's gig which a certain act is the headline. If so, we add this name to the actname column.
            */
            PreparedStatement searchRC = conn.prepareStatement("SELECT * FROM preparedRC");
            ResultSet getRC = searchRC.executeQuery();
            String RC[][] = convertResultToStrings(getRC);
            checkRC.close();
            searchRC.close();
            getRC.close();
            maintainCheck(conn);
            return RC;
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    /* [Option 8: Economically Feasible Gigs]: This option is to provide a list of economically feasible gigs.
    Economically Feasible Gig Definition: a gig can reimburse the venue hirecost and the act standardfee by selling ticket of 
        average price within the venue capacity limit.
    Proportion of tickets Definition: amount of tickets required to reimburse total expense (hirecost and standardfee) / venue capacity.
    */
    public static String[][] option8(Connection conn){
        try{
            maintainCheck(conn);

            String feasibleGig[][] = new String[0][0];
            // SQL parameter: PROCEDURE searchFeasibleGig().
            PreparedStatement searchFeasibleGig = conn.prepareStatement("CALL searchFeasibleGig()");
            searchFeasibleGig.execute();
            /* VIEW sortedFeasibleGig includes:
            (1) venuename: name of the venue which can be economically feasible with an act of (2)actname.
            (2) actname: name of the act which can be economically feasible in venue of (1)venuename.
            (3) ticket_required: amount of tickets of average price which can reimburse the total cost (hirecost and standardfee).
            There are some intermediate views used during this process, please turn to VIEW preparedRC in shema.sql for details.

            To get (3) ticket_required, first we create a VIEW actVenue by cross joining act and venue, so we can derive the
                total cost (venue's hirecost and act's standardfee). Then we calculate the average price of ticket (of gigs which are 
                not cancelled). Based on the average price, we can calculate the maximum income (average price * capacity). We use this
                income to minus the total cost, which is the pure interest. As the option aims to "get even", we need to get the amount
                of tickets required to reimburse the total cost. Therefore, we divide the pure interest by the average price of tickets,
                which means the maximum amount of tickets that we do not need to "get even". After that, we use capacity to minus this
                maximum amount of tickets we don't need and then get the least amount of tickets that we need to "get even". Notice that
                a ceiling is required here as decimal digits does not work for the amount of ticket.
            */
            PreparedStatement getFeasibleGig = conn.prepareStatement("SELECT * FROM sortedFeasibleGig");
            ResultSet resultFeasibleGig = getFeasibleGig.executeQuery();
            feasibleGig = convertResultToStrings(resultFeasibleGig);
            maintainCheck(conn);
            return feasibleGig;
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }        
        return null;
    }

    // This method will check before and after a call of options based on the instructed criteria.
    public static void maintainCheck(Connection conn){
        try{
            /* SQL parameter: PROCEDURE checkAllCriteria().
            This SQL execution loops all gigs which are not cancelled, and then call FUNCTION checkCriteria(gigID) to see whether
            a gig's current status breaks the following criterias:
            (1) TIME CONFLICT: act's performance overlaps or act starts before the gig date.
            (2) TIME INTERVAL TOO LONG: act's performance gap is larger than 20 minutes or the first act starts 20 minutes later than the gig date.
            (3) ACT OVERTIME: an act plays longer than 2 hours.
            (4) DATE CROSSED: acts in a given gig plays on different date (which means crossing the midnight).
            (5) VENUE OVERLOAD: the ticket sold is greater than the venue capacity.*/
            PreparedStatement checkCriteria = conn.prepareStatement("CALL checkAllCriteria()");
            checkCriteria.execute();
            checkCriteria.close();
        }catch(SQLException e){
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * Prompts the user for input
     * @param prompt Prompt for user input
     * @return the text the user typed
     */

    private static String readEntry(String prompt){
        try {
            StringBuffer buffer = new StringBuffer();
            System.out.print(prompt);
            System.out.flush();
            int c = System.in.read();
            while(c != '\n' && c != -1) {
                buffer.append((char)c);
                c = System.in.read();
            }
            return buffer.toString().trim();
        } catch (IOException e) {
            return "";
        }
    }

    private static boolean checkreadEntryInt(String entry){
        
        try{
            int test = Integer.parseInt(entry);
            return true;
        }catch(NumberFormatException e){
            System.out.println("Please enter a number.");
        }
        return false;
    }
     
    /**
    * Gets the connection to the database using the Postgres driver, connecting via unix sockets
    * @return A JDBC Connection object
    */
    public static Connection getSocketConnection(){
        Properties props = new Properties();
        props.setProperty("socketFactory", "org.newsclub.net.unix.AFUNIXSocketFactory$FactoryArg");
        props.setProperty("socketFactoryArg",System.getenv("HOME") + "/cs258-postgres/postgres/tmp/.s.PGSQL.5432");
        Connection conn;
        try{
          conn = DriverManager.getConnection("jdbc:postgresql://localhost/cwk", props);
          return conn;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Gets the connection to the database using the Postgres driver, connecting via TCP/IP port
     * @return A JDBC Connection object
     */
    public static Connection getPortConnection() {
        
        String user = "postgres";
        String passwrd = "password";
        Connection conn;

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException x) {
            System.out.println("Driver could not be loaded");
        }

        try {
            conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/cwk?user="+ user +"&password=" + passwrd);
            return conn;
        } catch(SQLException e) {
            System.err.format("SQL State: %s\n%s\n", e.getSQLState(), e.getMessage());
            e.printStackTrace();
            System.out.println("Error retrieving connection");
            return null;
        }
    }

    public static String[][] convertResultToStrings(ResultSet rs){
        Vector<String[]> output = null;
        String[][] out = null;
        try {
            int columns = rs.getMetaData().getColumnCount();
            output = new Vector<String[]>();
            int rows = 0;
            while(rs.next()){
                String[] thisRow = new String[columns];
                for(int i = 0; i < columns; i++){
                    thisRow[i] = rs.getString(i+1);
                }
                output.add(thisRow);
                rows++;
            }
            // System.out.println(rows + " rows and " + columns + " columns");
            if(rows > 0){
                out = new String[rows][columns];
                for(int i = 0; i < rows; i++){
                    out[i] = output.get(i);
                }
            }
            else{
                System.out.println("NO ROW TO PRINT.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
    }

    public static void printTable(String[][] out){
        try {
            if(out != null){
                int numCols = out[0].length;
                int w = 20;
                int widths[] = new int[numCols];
                for(int i = 0; i < numCols; i++){
                    widths[i] = w;
                }
                printTable(out,widths);
            }
        } catch (Exception e){}
    }

    public static void printTable(String[][] out, int[] widths){
        for(int i = 0; i < out.length; i++){
            for(int j = 0; j < out[i].length; j++){
                System.out.format("%"+widths[j]+"s",out[i][j]);
                if(j < out[i].length - 1){
                    System.out.print(",");
                }
            }
            System.out.println();
        }
    }
}