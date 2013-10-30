package load_shedding_sim;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TrendAnalysis {
	File fileOutputResults;
	public Connection connectDB() {
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/postgres", "postgres",
					"123456");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
			return connection;
		} else {
			System.out.println("Failed to make connection!");
			return null;
		}
	}
	public String countingFuture (ResultSet rs, DataEntry inputEntry) {
		int totalInput;
		if(Debug.ORACLE_EPG)
			totalInput = 115861;
		else
			totalInput = 12007002;
		
		//int MAX_ARRAY_INDEX = 15;
		
		int[][] counting = new int[Debug.ORACLE_MAX_ARRAY_INDEX][1];
		
		try {
			 while ( rs.next() ) {
	             String simTimeString = rs.getString("sim_time");
	             int resultSimTime = Integer.parseInt(simTimeString);
	             int entrySimTime = inputEntry.simTimeStamp;
	             int tmp = resultSimTime-entrySimTime;
	             double precentage = (double) (tmp) / (double)(totalInput);
	             //System.out.println(precentage +"\t" + tmp+"\t" + resultSimTime+"\t" + entrySimTime);
	             int arrayIndex = 0;
	             if(precentage < 0.002)
	            	 arrayIndex = 0;
	             else if(precentage < 0.004)
	            	 arrayIndex = 1;
	             else if(precentage < 0.006)
	            	 arrayIndex = 2;
	             else if(precentage < 0.008)
	            	 arrayIndex = 3;
	             else if(precentage < 0.01)
	            	 arrayIndex = 4;
	             else if(precentage < 0.02)
	            	 arrayIndex = 5;
	             else if(precentage < 0.04)
	            	 arrayIndex = 6;
	             else if(precentage < 0.06)
	            	 arrayIndex = 7;
	             else if(precentage < 0.08)
	            	 arrayIndex = 8;
	             else if(precentage < 0.10)
	            	 arrayIndex = 9;
	             else if(precentage < 0.20)
	            	 arrayIndex = 10;
	             else if(precentage < 0.40)
	            	 arrayIndex = 11;
	             else if(precentage < 0.60)
	            	 arrayIndex = 12;
	             else if(precentage < 0.80)
	            	 arrayIndex = 13;
	             else
	            	 arrayIndex = 14;
	             for(int i = arrayIndex; i < Debug.ORACLE_MAX_ARRAY_INDEX; i++)
	            	 counting[i][0]++;
	         }
			 
			 StringBuffer buf = new StringBuffer();
			 buf.append(inputEntry.simTimeStamp+"\t"+Debug.sdf.format(inputEntry.timeStamp).toString()+"\t"+Debug.sdf.format(inputEntry.timeStampEnd).toString()+"\t"+inputEntry.key+"\t"+inputEntry.otherDataFields);
             for(int i = 0; i < Debug.ORACLE_MAX_ARRAY_INDEX; i++)
            	 buf.append("\t"+Integer.toString(counting[i][0]));
             //System.err.println(buf.toString());
             buf.append("\n");
             //System.out.println(buf.toString());
             Files.append(buf.toString(), fileOutputResults, Charsets.UTF_8);
			 return buf.toString();
			 
		}catch (Exception e) {
			System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
		}
		return null;
	}
	public void making () {
		Connection connection = connectDB();
		try {
			File fileInput;
			if(Debug.ORACLE_EPG)
				fileInput = new File("input//epg_sim_time.csv");
			else
				fileInput = new File("input//log_sim_time.csv");
			
			Scanner fileScanner = new Scanner(fileInput);
			int counter = 0;
			
			if(Debug.ORACLE_EPG)
				fileOutputResults = new File("output//epg_with_oracle"+".csv");
			else
				fileOutputResults = new File("output//log_with_oracle"+".csv");
	    	
			if (fileOutputResults.exists())
	    		fileOutputResults.delete();
	    		
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
		        countingFuture (rs,  newEntry);     
	            counter++;
			} 
			System.out.println("done");
            connection.close();
        } catch (Exception e) {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }
		
	}

	public void inputAnalysis () {
		//Debug.sdf.
		
	}
}
	

