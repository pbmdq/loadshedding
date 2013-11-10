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
		File input = new File("input//wind_speed_with_oracle.csv");
		File output = new File("output//analysis_SRbench_oracle.csv"); 
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
}
	

