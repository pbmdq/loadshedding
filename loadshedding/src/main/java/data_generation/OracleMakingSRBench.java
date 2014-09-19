package data_generation;

import java.io.File;
import java.sql.*;

import load_shedding_sim.Debug;
import data_entry.DataEntry;
import data_entry.DataEntrySRBench;

public class OracleMakingSRBench extends OracleMaking{
	public OracleMakingSRBench () {
		super(3);
	}
	public void making () {
		Connection connection = connectDB();
		try {
			int counter = 0;
			while (fileScanner.hasNext() ) {
				String inputStrig 		= fileScanner.nextLine();
				DataEntry newEntry 		= new DataEntrySRBench (inputStrig, 0);
				Statement stmt 			= connection.createStatement();
	            Long startTime 			= newEntry.timeStamp.getTime();
	        	String SQL				= null;
	        	ResultSet rs;
	        	switch (inputID) {
					case Debug.ORACLE_SR_WINDSPEED :
						SQL = "SELECT * FROM windspeed_simtime where abs( time - " +startTime+ " )< 200 AND sersorid != '"+newEntry.otherDataFields+"' AND measurment = " +newEntry.key+" AND measurment >= 10"+" ;";
		            	break;
	        	}
	        	
	        	//System.out.println(SQL);
	        	rs = stmt.executeQuery(SQL);
		        countingFuture (rs, newEntry);     
	            counter++;
			} 
			System.out.println("done");
            connection.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
		
	}
}
	

