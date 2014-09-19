package data_generation;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import load_shedding_sim.Debug;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import data_entry.DataEntryVistaTV;

public class OracleMakingVistaTV extends OracleMaking{
	public OracleMakingVistaTV ( int inputType) {
		super(inputType);
	}
	public void making () {
		Connection connection = connectDB();
		try {
			int counter = 0;
			Statement stmt = connection.createStatement();
			while (fileScanner.hasNext() ) {
				String inputStrig = fileScanner.nextLine();
				DataEntryVistaTV newEntry = new DataEntryVistaTV (inputStrig, 0);
				if(newEntry.timeStamp.after(Debug.sdf.parse("2012-08-05 00:22:07")))
					break;
				
				
	            ResultSet rs;
	            
	            String startTime =  Debug.sdf.format(newEntry.timeStamp).toString();
	            String endTime 	 =  Debug.sdf.format(newEntry.timeStampEnd).toString();
	            //String endTime =  sdf.format(newEntry.timeStampEnd).toString();
	        	String SQL;
	            if(this.inputID == Debug.ORACLE_EPG)
	            	SQL = "SELECT * FROM joinresults_local_sim_time where common_ch_name1 = '"+newEntry.key+"' AND start1 = '"+startTime+"' AND  endtime1 = '"+endTime+"' AND id = "+newEntry.otherDataFields;
	            else
	            	SQL = "SELECT * FROM joinresults_local_sim_time where name = '"+newEntry.otherDataFields+"' AND start2 = '"+startTime+"' AND  endtime2 = '"+endTime+ "' ;";
	            //System.out.println(SQL);
	            rs = stmt.executeQuery(SQL);
	            countingFutureVistaTV (rs,  newEntry);     
	            counter++;
	            //System.out.println(counter);
			} 
			System.out.println("done");
            connection.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
		
	}
}
	

