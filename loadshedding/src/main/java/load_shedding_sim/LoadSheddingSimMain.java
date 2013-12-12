package load_shedding_sim;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

import org.apache.commons.cli.*;
//import org.apache.commons.cli.Parser;
import org.apache.commons.lang3.time.DateUtils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class LoadSheddingSimMain {
	public static void main (String[] args) throws Exception{
     
		JoinOperation myJoinOperation = new JoinOperation(args);
		
		//OracleMakingSRBench myOracle = new OracleMakingSRBench();
		
		//OracleMakingVistaTV myOracle = new OracleMakingVistaTV(Debug.ORACLE_LOG);
		//myOracle.making();
		
		//AnalysisInput myAnalysis = new AnalysisInput();
		//myAnalysis.getHistogram();
		//myAnalysis.getHistogramWithLocalSimTime();
		
		//myAnalysis.windSpeedFilter();
		
		
		//DataPreparing myDataPrepare = new DataPreparing();
		//myDataPrepare.addGlobalSimTime();
		
		/*
		 * original input
		 * -T 1 -I //Users//shengao//git//loadshedding//loadshedding//input//epg_with_oracle.csv -O //Users//shengao//git//loadshedding//loadshedding//input//log_with_oracle.csv -OUT //Users//shengao//git//loadshedding//loadshedding//output// -Si 1 -So 0.02 -L 1 -LRU
		 * 
		inputArguments = parserArguments(args);
		String innerDir;
		String outterDir;
		int innerSize;
		int outterSize;
		int length = 0;
		String type;
		int oracleDepth = 0;
		
		
		innerDir 	= inputArguments.getOptionValue("innerDir") ;
		System.out.println( inputArguments.getOptionValue( "innerDir" ) );
		outterDir 	= inputArguments.getOptionValue("outterDir") ;
		System.out.println( inputArguments.getOptionValue( "outterDir" ) );

		double innerRatio = Double.parseDouble(inputArguments.getOptionValue("innerSize") );
		innerSize 	= (int)(innerRatio*(double)(Debug.EPG_MAX_SIZE));
		System.out.println( innerSize );
		double outterRatio= Double.parseDouble(inputArguments.getOptionValue("onnerSize") );
		outterSize 	= (int)(outterRatio*(double)(Debug.LOG_MAX_SIZE));
		System.out.println( outterSize );
		
		
		outputDir 	= inputArguments.getOptionValue("outputDir");
		java.util.Date date= new java.util.Date();
		outputDir 	= outputDir.concat(String.valueOf(date.getTime()));
		outputDir 	= outputDir.concat("//");
		//Files.createParentDirs(new File(outputDir));
		new File(outputDir).mkdir();
		
		length 		= Integer.parseInt(inputArguments.getOptionValue("length") );
				
		if( inputArguments.hasOption( "ORACLE" ) ) {
			type = "ORACLE";
			oracleDepth = Integer.parseInt(inputArguments.getOptionValue("ORACLE") );
		} else if (inputArguments.hasOption( "LRU" ) ) {
			type = "LRU";
		} else if (inputArguments.hasOption( "CLOCK" ) ) {
			type = "CLOCK";
		} else {
			throw new Exception();
		}
		
		LoadSheddingSimMain myLoadSheddingSim = new LoadSheddingSimMain (innerDir, innerSize, outterDir, outterSize, length, type, oracleDepth );
		
		Files.append(type+"\t"+oracleDepth+"\t"+innerSize+"\t"+outterSize+"\t"+length+"\n", overallResults, Charsets.UTF_8);
		myLoadSheddingSim.twoWayJoinSim();
		
		*/
		//myLoadSheddingSim.printInputWriteSimTimeStamp();
		
		//OracleMakingSRBench myOracle = new OracleMakingSRBench();
		//OracleMaking myOracle = new OracleMaking();
		//myOracle.making();
		
		//SRBenchPreparing mySRBench = new SRBenchPreparing();
		//mySRBench.copyOneMeasurment();
		//mySRBench.mergeFiles();
		//mySRBench.addSimTime();

		
	}
}

