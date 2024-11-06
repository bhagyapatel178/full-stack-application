/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2565267
 *
 */

//import java.io.OutputStreamWriter;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import java.io.*;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
    //these clasess are not exported by the module. Instead, one needs to impor
    //javax.sql.rowset.* as above.


public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome   = null;

	//JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL      = Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        
		//TO BE COMPLETED
        this.serviceSocket = aSocket;

        this.start();
		
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest()
    {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop
		
		String tmp = "";
        try {

			//TO BE COMPLETED

            InputStream inputStream = this.serviceSocket.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            StringBuffer stringBuffer = new StringBuffer();
            char x;
            while(true) {
                x = (char) inputStreamReader.read();
                if (x == '#')
                    break;
                stringBuffer.append(x);
            }

            String combinedWords = stringBuffer.toString();

                String[] seperateWords = combinedWords.split(";");
                if(seperateWords.length == 2) {
                    this.requestStr[0] = seperateWords[0];
                    this.requestStr[1] = seperateWords[1];
                }


         }catch(IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;
		
		String sql = "SELECT r.title, r.label, g.name, r.rrp,COUNT(*) AS copyid FROM record r INNER JOIN artist a ON r.artistid = a.artistid INNER JOIN genre g ON r.genre = g.name INNER JOIN recordcopy rc ON r.recordid = rc.recordid INNER JOIN recordshop rs ON rc.recordshopid = rs.recordshopid WHERE a.lastname = ? AND rs.city = ? GROUP BY r.title, r.label, g.name, r.rrp;"; //TO BE COMPLETED- Update this line as needed.
		
		
		try {
			//Connet to the database
			//TO BE COMPLETED
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(Credentials.URL,Credentials.USERNAME,Credentials.PASSWORD);

			
			//Make the query
			//TO BE COMPLETED
            PreparedStatement thequery = connection.prepareStatement(sql);

            thequery.setString(1,requestStr[0] );
            thequery.setString(2,requestStr[1] );

            ResultSet resultSet = thequery.executeQuery();
			//Process query
			//TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();
            crs.populate(resultSet);
            this.outcome = crs;

            while(crs.next()) {
                System.out.println(crs.getString("title") + " | " + crs.getString("label") + " | " + crs.getString("name") + " | " + crs.getString("rrp") + " | " + crs.getString("copyid"));
            }
            crs.beforeFirst();

			//Clean up
			//TO BE COMPLETED
            resultSet.close();
            thequery.close();
            connection.close();

			
		} catch (Exception e)
		{ System.out.println(e); }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public void returnServiceOutcome(){
        try {
			//Return outcome
			//TO BE COMPLETED
            ObjectOutputStream outputStreamWriter = new ObjectOutputStream(serviceSocket.getOutputStream());
            outputStreamWriter.writeObject(outcome);
			
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);
            
			//Terminating connection of the service socket
			//TO BE COMPLETED
			outputStreamWriter.close();
            serviceSocket.close();
			
        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
