package load_shedding_sim;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class OracleMaking {
	File fileOutputResults;
	File fileInput;
	int totalInput;
	Scanner fileScanner;
	int inputID;
	public OracleMaking (int input) {
		switch (input) {
			case Debug.ORACLE_EPG1 :
				fileInput 			= new File("input//epg_sim_time.csv");
				fileOutputResults 	= new File("output//epg_with_oracle"+".csv");
				totalInput			= Debug.EPG_MAX_SIZE;
				break;
			case Debug.ORACLE_LOG :
				fileInput 			= new File("input//log_sim_time.csv");
				fileOutputResults 	= new File("output//log_with_oracle"+".csv");
				totalInput			= Debug.LOG_MAX_SIZE;
				break;
			case Debug.ORACLE_SR_WINDSPEED :
				fileInput 			= new File("input//wind_speed_simTime.csv");
				fileOutputResults 	= new File("output//wind_speed_with_oracle"+".csv");
				totalInput			= Debug.WINDSPEED_MAX_SIZE;
				break;
		}
		inputID = input;
		if (fileOutputResults.exists())
    		fileOutputResults.delete();
		try {
			fileScanner = new Scanner(fileInput);
		}catch (Exception e) {}
	}
	public Connection connectDB() {
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
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

	
	
	public String countingFuture (ResultSet rs, DataEntryBase inputEntry) {
		
		int[][] counting = new int[Debug.ORACLE_MAX_ARRAY_INDEX][1];
		int[]   compare  = new int[Debug.ORACLE_MAX_ARRAY_INDEX];
		for(int i = 0; i < Debug.ORACLE_MAX_ARRAY_INDEX; i++) {
			compare[i] = (i+1)*100;
		}
		try {
			 while ( rs.next() ) {
	             String simTimeString = rs.getString("sim_time");
	             int resultSimTime = Integer.parseInt(simTimeString);
	             int entrySimTime = inputEntry.simTimeStamp;
	             int tmp = resultSimTime-entrySimTime;
	             tmp = (tmp < 0) ? -tmp : tmp;
	             //double precentage = (double) (tmp); /// (double)(totalInput);
	             //System.out.println(precentage +"\t" + tmp+"\t" + resultSimTime+"\t" + entrySimTime);
	             int arrayIndex = 0;
	             for(int i = 0; i < Debug.ORACLE_MAX_ARRAY_INDEX-1; i++) {
	            	if(tmp < compare[i])
	            		break;
	            		arrayIndex++;
	     			//compare[i] = (i+1)*100;
	     		}
	            counting[arrayIndex][0]++;
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
	public void making () {}
}
	

