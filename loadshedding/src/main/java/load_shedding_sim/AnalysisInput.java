package load_shedding_sim;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.io.*;

import com.google.common.base.Charsets;
import com.google.common.io.*;

public class AnalysisInput {
	// analysis the oracle input and draw the historagm of results producing
	public void getHistogram () {
		File input = new File("output//epg_with_oracle.csv");
		File output = new File("output//Analysis_epg_with_oracle.csv"); 
		Scanner fileScanner = null;
		int [] histogram = new int[Debug.ORACLE_MAX_ARRAY_INDEX];
		
		try {
			fileScanner = new Scanner(input);
			while (fileScanner.hasNext() ) {
				String inputStrig 		= fileScanner.nextLine();
				String[]  afterSplit = inputStrig.split("\t");
				for(int i = 0; i< Debug.ORACLE_MAX_ARRAY_INDEX; i++) {
					histogram[i] += Integer.parseInt(afterSplit[5+i]);
				}
			}
			Files.append("intput file is" + input.getName() +"\n", output, Charsets.UTF_8);
			for(int i = 0; i< Debug.ORACLE_MAX_ARRAY_INDEX; i++) {
				Files.append(histogram[i] +"\t", output, Charsets.UTF_8);
			}
			Files.append("\n" + input.getName() +"\n", output, Charsets.UTF_8);
		}catch (Exception e) {}
	}
	public void windSpeedFilter(){
		File input = new File("input//wind_speed_with_oracle.csv");
		File outputEPG = new File("output//analysis_wind_speed_not_zero.csv"); 
		
		Scanner fileScanner = null;
		
		try {
			fileScanner = new Scanner(input);
			int counter = 0;
			while (fileScanner.hasNext() && counter<100000 ) {
				String inputStrig 		= fileScanner.nextLine();
				String[]  afterSplit = inputStrig.split("\t");
				int test1 = Integer.parseInt(afterSplit[5]);
				int test2 = Integer.parseInt(afterSplit[6]);
				
				if(test1 != 0 && test2 !=0) {
					Files.append(inputStrig+"\n", outputEPG, Charsets.UTF_8);
				} 
				counter++;
				
			}
			
		}catch (Exception e) {}
	
	}
	public void getHistogramWithLocalSimTime () {
		File input = new File("input//LRU_resutls_EPG_Log_LocalSimTime.txt");
		File outputEPG = new File("output//analysis_epg_histogram.csv"); 
		File outputLOG = new File("output//analysis_log_histogram.csv"); 
		
		Scanner fileScanner = null;
		int [] histogramEPG = new int[Debug.ORACLE_MAX_ARRAY_INDEX];
		int [] histogramLOG = new int[Debug.ORACLE_MAX_ARRAY_INDEX];
		
		try {
			fileScanner = new Scanner(input);
			int counter = 0;
			while (fileScanner.hasNext() && counter<100000 ) {
				String inputStrig 		= fileScanner.nextLine();
				String[]  afterSplit = inputStrig.split(",");
				int entryTime = Integer.parseInt(afterSplit[afterSplit.length-2]);
				int cacheTime = Integer.parseInt(afterSplit[afterSplit.length-1]);
				int timeDifferent = 0;
				int unit = 1000;
				if(entryTime == cacheTime) {
					timeDifferent = Integer.parseInt(afterSplit[afterSplit.length-3])-Integer.parseInt(afterSplit[afterSplit.length-4]);;
					unit = 100;
					
					if(timeDifferent/unit<Debug.ORACLE_MAX_ARRAY_INDEX-1)
						histogramEPG[timeDifferent/unit]++;
					else
						histogramEPG[Debug.ORACLE_MAX_ARRAY_INDEX-1]++;
				} else {	
					timeDifferent = cacheTime-entryTime;
					unit = 5000;
					
					if(timeDifferent/unit<Debug.ORACLE_MAX_ARRAY_INDEX-1)
						histogramLOG[timeDifferent/unit]++;
					else
						histogramLOG[Debug.ORACLE_MAX_ARRAY_INDEX-1]++;
					
				}
				counter++;
				
			}
			Files.append("intput file is"+"\n", outputEPG, Charsets.UTF_8);
			for(int i = 0; i< Debug.ORACLE_MAX_ARRAY_INDEX; i++) {
				Files.append(histogramEPG[i] +"\t", outputEPG, Charsets.UTF_8);
			}
			Files.append("\n"  +"\n", outputEPG, Charsets.UTF_8);
			
			Files.append("intput file is" +"\n", outputLOG, Charsets.UTF_8);
			for(int i = 0; i< Debug.ORACLE_MAX_ARRAY_INDEX; i++) {
				Files.append(histogramLOG[i] +"\t", outputLOG, Charsets.UTF_8);
			}
			Files.append("\n" +"\n", outputLOG, Charsets.UTF_8);
			
		}catch (Exception e) {}
	}
}
	

