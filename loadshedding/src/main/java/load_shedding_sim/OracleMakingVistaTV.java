package load_shedding_sim;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class OracleMakingVistaTV extends OracleMaking{
	public OracleMakingVistaTV () {
		super(1);
	}
	public void making () {
		Connection connection = connectDB();
		try {
			int counter = 0;
			while (fileScanner.hasNext() ) {
				String inputStrig = fileScanner.nextLine();
				DataEntry newEntry = new DataEntry (inputStrig, 0);
				
				Statement stmt = connection.createStatement();
	            ResultSet rs;
	            
	            String startTime =  Debug.sdf.format(newEntry.timeStamp).toString();
	            String endTime 	 =  Debug.sdf.format(newEntry.timeStampEnd).toString();
	            //String endTime =  sdf.format(newEntry.timeStampEnd).toString();
	        	String SQL;
	            if(Debug.ORACLE_EPG)
	            	SQL = "SELECT * FROM joinresults_sim_time where common_ch_name1 = '"+newEntry.key+"' AND start1 = '"+startTime+"' AND  endtime1 = '"+endTime+"' AND id = "+newEntry.otherDataFields;
	            else
	            	SQL = "SELECT * FROM joinresults_sim_time where name = '"+newEntry.otherDataFields+"' AND start2 = '"+startTime+"' AND  endtime2 = '"+endTime+ "' ;";
	            //System.out.println(SQL);
	            rs = stmt.executeQuery(SQL);
		        //countingFuture (rs,  newEntry);     
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
	

