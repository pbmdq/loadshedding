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
	//public DataCache innerCache;
	//public DataCache outerCache;
	
	public DataCache innerCache;
	public DataCache outerCache;
	
	public int outputCounter;
	public int executionLenghth;
	public Date endingTime;
	public int simTimeStamp;
	
	public OracleMaking myConnection;
	
	static CommandLine inputArguments;
	static CommandLineParser parser = new GnuParser();
	static Options options = new Options();
	
	static String outputDir;
	
	static File overallResults;
	
	public LoadSheddingSimMain (String innerTableDir, int innerCacheSize, String outerTableDir, int outterCacheSize, int executionLength, String type, int oracleDepth) throws Exception {
		if( type.equals("ORACLE") ) {
			innerCache = new DataCacheKhoa (innerTableDir, innerCacheSize, true, outputDir, oracleDepth);
			outerCache = new DataCacheKhoa (outerTableDir, outterCacheSize, false, outputDir, oracleDepth);
		} else if (type.equals( "LRU" ) ) {
			innerCache = new DataCacheLRU (innerTableDir, innerCacheSize, true, outputDir);
			outerCache = new DataCacheLRU (outerTableDir, outterCacheSize, false, outputDir);
		} else if (type.equals( "CLOCK" ) ) {
			innerCache = new DataCacheClock (innerTableDir, innerCacheSize, true, outputDir);
			outerCache = new DataCacheClock (outerTableDir, outterCacheSize, false, outputDir);
		}
		this.executionLenghth= executionLength;
		this.simTimeStamp = 0;
		overallResults = new File(outputDir+"overall_results.csv");
	}

	public void twoWayJoinSim( ) throws Exception{
		System.out.println("Start");
		DataEntry innerEntry 	= innerCache.next(simTimeStamp++);
		DataEntry outterEntry 	= outerCache.next(simTimeStamp++);
		endingTime = DateUtils.addDays(innerEntry.timeStamp, this.executionLenghth);
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(endingTime) || outterEntry.timeStamp.before(endingTime)) ) {
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				if( outerCache.store.size()>0 ) {
					int numOfJoinResults = outerCache.performJoin(innerEntry, innerCache);
					if(numOfJoinResults>0) {
						outputCounter +=numOfJoinResults;
					}
				}
				innerEntry = innerCache.next(simTimeStamp);
			} else {
				int numOfJoinResults = innerCache.performJoin(outterEntry, outerCache);
				if(numOfJoinResults>0) {
					outputCounter +=numOfJoinResults;
				}
				outterEntry = outerCache.next(simTimeStamp);
			}
			simTimeStamp++;
			
		}
		innerCache.endOfCache();
		outerCache.endOfCache();
		Files.append(outputCounter+"\n", overallResults, Charsets.UTF_8);
		
		System.out.println("End of two way join");
		System.out.println(outputCounter);
	
	}
	public void printInputWriteSimTimeStamp() throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DataEntry innerEntry = innerCache.next(++simTimeStamp);
		File file = new File("output//epg_sim_time.csv");
	    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
		output.write(simTimeStamp+"\t"+sdf.format(innerEntry.timeStamp).toString()+"\t"+sdf.format(innerEntry.timeStampEnd).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields+"\n");
	    
	    DataEntry outterEntry = outerCache.next(++simTimeStamp);
		File file1 = new File("output//log_sim_time.csv");
	    BufferedWriter output1 = new BufferedWriter(new FileWriter(file1, true));
		output1.write(simTimeStamp+"\t"+sdf.format(outterEntry.timeStamp).toString()+"\t"+sdf.format(outterEntry.timeStampEnd).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields+"\n");
	    
		endingTime = DateUtils.addDays(innerEntry.timeStamp, executionLenghth);
		
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(endingTime) || outterEntry.timeStamp.before(endingTime))) {
			simTimeStamp++;
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				innerEntry = innerCache.next(simTimeStamp);
				if(innerEntry != null)
					output.write(simTimeStamp+"\t"+sdf.format(innerEntry.timeStamp).toString()+"\t"+sdf.format(innerEntry.timeStampEnd).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields+"\n");
			} else {
				outterEntry = outerCache.next(simTimeStamp);
				if(outterEntry != null)
					output1.write(simTimeStamp+"\t"+sdf.format(outterEntry.timeStamp).toString()+"\t"+sdf.format(outterEntry.timeStampEnd).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields+"\n");
			}
			
		}
		output.close();
		output1.close();
		
		System.out.println("End of print");
	}
	private static void usage(Options options){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "CLIDemo", options );
	}
	private static CommandLine parserArguments (String[] args) throws Exception{
		options.addOption( "I", "innerDir",  true, "inner cache  Dir");
		options.addOption( "O", "outterDir", true, "outter cache Dir");
		options.addOption( "Si", "innerSize",  true, "inner cache  Size");
		options.addOption( "So", "onnerSize", true, "outter cache Size");
		options.addOption( "L", "length", true, "legnth of experiment in days");
		options.addOption( "ORACLE", "oracle", true, "oracle experiments");
		options.addOption( "LRU", "lru", false, "lru experiments");
		options.addOption( "CLOCK", "clock", false, "clock experiments");
		options.addOption( "OUT", "outputDir", true, "output Dir");
		try {
			inputArguments = parser.parse( options, args );
			return inputArguments;
		}catch (ParseException pe){ usage(options); return null; }
	}
	public static void main (String[] args) throws Exception{
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
		
		//myLoadSheddingSim.printInputWriteSimTimeStamp();
		
		//OracleMaking myOracle = new OracleMaking();
		//myOracle.making();
	}
}

