package load_shedding_sim;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.io.*;

import com.google.common.base.Charsets;
import com.google.common.io.*;

// access all directory and concatenate all the files into one

public class SRBenchPreparing {
	public void mergeFiles () {
		File mergeBase = new File("//Users//shengao//Documents//Research_data//SR-Bench_data//charley//WindSpeed//");
		File destFile  = new File("//Users//shengao//Documents//Research_data//SR-Bench_data//charley//WindSpeed.csv");
		
		File[] myarray;
		myarray=mergeBase.listFiles();
		for (int j = 0; j < myarray.length; j++)
		{
			System.out.println(myarray[j].toString());
			try{
				List<String> temp = Files.readLines(myarray[j], Charsets.UTF_8);
			} catch (Exception e) {}
		}
	}
	
	public void addSimTime () {
		File input = new File("input//wind_speed_ordered.csv");
		File output = new File("input//wind_speed_simTime.csv"); 
		Scanner fileScanner = null;
		int simTime = 1;
		try {
			fileScanner = new Scanner(input);
		}catch (Exception e) {}
		while (fileScanner.hasNext() ) {
			String inputStrig 		= fileScanner.nextLine();
			try {
				Files.append(simTime+","+inputStrig+"\n", output, Charsets.UTF_8);
			}catch (Exception e) {} 
			simTime++;
		} 
	}
	
	public void copyOneMeasurment () {
		File directory = new File("//Users//shengao//Documents//Research_data//SR-Bench_data//charley//extracted//");   
		//File destDir   = new File("//Users//shengao//Documents//Research_data//SR-Bench_data//charley//WindSpeed//");
		String destBase= "//Users//shengao//Documents//Research_data//SR-Bench_data//charley//WindSpeed//";
		File[] myarray;
		myarray=directory.listFiles();
		for (int j = 0; j < myarray.length; j++)
		{
			System.out.println(myarray[j].toString());
			if(myarray[j].isDirectory()) {
				File[] measurmentsArray;
				measurmentsArray=myarray[j].listFiles();
				for (int i = 0; i < measurmentsArray.length; i++)
				{
					if(measurmentsArray[i].toString().contains("WindSpeed")) {
						//System.out.println(measurmentsArray[i].toString());
						try{ 
							File[] csvFile = measurmentsArray[i].listFiles();
							//csvFile[0].re
							//FileUtils.copyFileToDirectory(csvFile[0], destDir);
							String destDir = destBase.concat(myarray[j].getName()+".csv");
							System.out.println(destDir);
							Files.copy(csvFile[0], new File (destDir));
						} catch (Exception e) {
							
						}
					}
				}
			}
		       
		}
	}
}
	

