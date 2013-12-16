package load_shedding_sim;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

import org.apache.commons.cli.*;
//import org.apache.commons.cli.Parser;
import org.apache.commons.lang3.time.DateUtils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class JoinOperation {

	public DataCache innerCache;
	public DataCache outerCache;
	
	public int outputCounter;
	public int executionLenghth;
	public Date endingTime;
	public int simTimeStamp;
	
	static CommandLine inputArguments;
	static CommandLineParser parser = new GnuParser();
	static Options options = new Options();
	
	static String outputDir;
	
	static File overallResults;
	int joinType;
	
	int numOfWarmUp;
	boolean isWarmingUp;
	
	public Date currentSystemReadTimeStamp;
	
	public class TimeLineStat{
		public int currentSlotInnerInput;
		public int currentSlotOutterInput;
		public int currentSlotTotalResutls;
		
		public int totalSlotInnerInput;
		public int totalSlotOutterInput;
		public int totalSlotTotalResutls;
		
		public int totalSlotNum;
		public int sizeOfSlot; // in term of secs for now
		
		public Date slotStartingTimeStamp;
		
		public TimeLineStat( int sizeOfSlot){
			this.sizeOfSlot = sizeOfSlot;
			totalSlotNum = 0;
		}
		public void startNewSlot (){
			this.totalSlotInnerInput 	+= this.currentSlotInnerInput;
			this.totalSlotOutterInput 	+= this.currentSlotOutterInput;
			this.totalSlotTotalResutls 	+= this.currentSlotTotalResutls;
			
			this.currentSlotInnerInput 		= 0;
			this.currentSlotOutterInput 	= 0;
			this.currentSlotTotalResutls 	= 0;
			
			this.slotStartingTimeStamp = DateUtils.addSeconds(this.slotStartingTimeStamp, this.sizeOfSlot);
		}
		public void collectStat (DataEntry inputEntry, int numOfResults){
			
		}
	}
	
	private static CommandLine parserArguments (String[] args) throws Exception{
		options.addOption( "T", "joinType",  true, "specify the join type (one-way,twp-way and Query)");
		/*
		 *  1X for SRBench one way join Query one
		 *  1 for SRBench windspeed self join
		 *  2X for Vista-Tv query
		 *  
		 */
		options.addOption( "I", "innerDir",  true, "inner cache  Dir");
		options.addOption( "O", "outterDir", true, "outter cache Dir");
		options.addOption( "Si", "innerSize",  true, "inner cache  Size");
		options.addOption( "So", "onnerSize", true, "outter cache Size");
		options.addOption( "L", "length", true, "legnth of experiment in days");
		options.addOption( "ORACLE", "oracle", true, "oracle experiments");
		options.addOption( "LRU", "lru", false, "lru experiments");
		options.addOption( "TRUELRU", "turelru", false, "not implemented as clock");
		options.addOption( "FIFO", "fifo", false, "fifo experiments");
		options.addOption( "CLOCK", "clock", false, "clock experiments");
		options.addOption( "FIFOCLOCK", "fifoclock", false, "fifo+clock experiments");
		options.addOption( "FIFOLRU", "fifolru", false, "fifo+lru experiments");
		options.addOption( "FIFOFIFO", "fifofifo", false, "fifo+fifos experiments");
		options.addOption( "SF", "sizeOfFIFO", true, "fifo area size");
		options.addOption( "TH", "threshold", true, "fifo to other threshold");
		options.addOption( "R", "enable reasoning", false, "enable reason or not");
		options.addOption( "W", "warmup", true, "warm up duration in terms of simtime");
		options.addOption( "OUT", "outputDir", true, "output Dir");
	
		try {
			inputArguments = parser.parse( options, args );
			return inputArguments;
		}catch (ParseException pe){ usage(options); return null; }
	}
	
	public JoinOperation (String[] args) throws Exception{
		inputArguments 	= parserArguments(args);
		String innerDir	= null;
		String outterDir= null;
		int innerSize	= 0;
		int outterSize	= 0;
		int length 		= 0;
		String type		= null;
		int oracleDepth = 0;
		
		joinType 	= Integer.parseInt(inputArguments.getOptionValue("joinType"));
		innerDir 	= inputArguments.getOptionValue("innerDir");
		outterDir 	= inputArguments.getOptionValue("outterDir");
		
		double innerRatio = Double.parseDouble(inputArguments.getOptionValue("innerSize") );
		innerSize = innerRatio<=1?(int)(innerRatio*(double)(Debug.EPG_MAX_SIZE)):(int)innerRatio;
		double outterRatio= Double.parseDouble(inputArguments.getOptionValue("onnerSize") );
		outterSize = outterRatio<=1?(int)(outterRatio*(double)(Debug.LOG_MAX_SIZE)):(int)outterRatio;
		
		outputDir 	= inputArguments.getOptionValue("outputDir");
		outputDir 	= outputDir.concat(String.valueOf((new java.util.Date()).getTime())+"//");
		new File(outputDir).mkdir();
		
		this.executionLenghth 	= Integer.parseInt(inputArguments.getOptionValue("length") );
				
		if( inputArguments.hasOption( "ORACLE" ) ) {
			type = "ORACLE";
			oracleDepth = Integer.parseInt(inputArguments.getOptionValue("ORACLE") );
		} else if (inputArguments.hasOption( "FIFO" ) ) {
			type = "FIFO";
		} else if (inputArguments.hasOption( "LRU" ) ) {
			type = "LRU";
		} else if (inputArguments.hasOption( "TRUELRU" ) ) {
			type = "TRUELRU";
		} else if (inputArguments.hasOption( "CLOCK" ) ) {
			type = "CLOCK";
		} else if (inputArguments.hasOption( "FIFOCLOCK" ) ) {
			type = "FIFOCLOCK";
		} else if (inputArguments.hasOption( "FIFOLRU" ) ) {
			type = "FIFOLRU";
		} else if (inputArguments.hasOption( "FIFOFIFO" ) ) {
			type = "FIFOFIFO";
		} 
		else {
			throw new Exception();
		}
		boolean enableReasoning = inputArguments.hasOption("R");
		
		if( type.equals("ORACLE") ) {
			innerCache = new DataCacheKhoa (innerDir, innerSize, true, enableReasoning, outputDir, oracleDepth);
			outerCache = outterDir==null?null:new DataCacheKhoa (outterDir, outterSize, false, enableReasoning, outputDir, oracleDepth);
		} else if (type.equals( "FIFO" ) ) {
			innerCache = new DataCacheFIFO (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheFIFO (outterDir, outterSize, false, enableReasoning, outputDir);
		} else if (type.equals( "TRUELRU" ) ) {
			innerCache = new DataCacheTRUELRU (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheTRUELRU (outterDir, outterSize, false, enableReasoning, outputDir);
		} else if (type.equals( "LRU" ) ) {
			innerCache = new DataCacheLRU (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheLRU (outterDir, outterSize, false, enableReasoning, outputDir);
		} else if (type.equals( "CLOCK" ) ) {
			innerCache = new DataCacheClock (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheClock (outterDir, outterSize, false, enableReasoning, outputDir);
		}else if (type.equals( "FIFOCLOCK" ) ) {
			double sizeFIFO = Double.parseDouble(inputArguments.getOptionValue("sizeOfFIFO"));
			int thresHold= Integer.parseInt(inputArguments.getOptionValue("TH"));
			innerCache = new DataCacheFIFOClock (innerDir, innerSize, true, enableReasoning, outputDir, sizeFIFO, thresHold);
			outerCache = outterDir==null?null:new DataCacheFIFOClock (outterDir, outterSize, false, enableReasoning, outputDir, sizeFIFO, thresHold);
		}else if (type.equals( "FIFOLRU" ) ) {
			double sizeFIFO = Double.parseDouble(inputArguments.getOptionValue("sizeOfFIFO"));
			int thresHold= Integer.parseInt(inputArguments.getOptionValue("TH"));
			innerCache = new DataCacheFIFOLRU (innerDir, innerSize, true, enableReasoning, outputDir, sizeFIFO, thresHold);
			outerCache = outterDir==null?null:new DataCacheFIFOLRU (outterDir, outterSize, false, enableReasoning, outputDir, sizeFIFO, thresHold);
		}else if (type.equals( "FIFOFIFO" ) ) {
			double sizeFIFO = Double.parseDouble(inputArguments.getOptionValue("sizeOfFIFO"));
			int thresHold= Integer.parseInt(inputArguments.getOptionValue("TH"));
			innerCache = new DataCacheFIFOFIFO (innerDir, innerSize, true, enableReasoning, outputDir, sizeFIFO, thresHold);
			outerCache = outterDir==null?null:new DataCacheFIFOFIFO (outterDir, outterSize, false, enableReasoning, outputDir, sizeFIFO, thresHold);
		}
		
		isWarmingUp = true;
		numOfWarmUp = Integer.parseInt(inputArguments.getOptionValue("warmup"));
		
		overallResults = new File(outputDir+"overall_results.csv");
		Files.append(type+"\t"+oracleDepth+"\t"+innerSize+"\t"+outterSize+"\t"+length+"\n", overallResults, Charsets.UTF_8);
		
		switch(joinType) {
		case Debug.JOIN_TYPE_ONE_WAY:
			this.oneWayJoinSim();
			break;
		case Debug.JOIN_TYPE_TWO_WAY:
		case 22:
			this.twoWayJoinSim();
			break;
		}
		
		
	}
	public void oneWayJoinSim() throws Exception{
		System.out.println("Start one way Join ");
		
		DataEntry innerEntry = innerCache.next(simTimeStamp++, joinType);
		endingTime = DateUtils.addDays(innerEntry.timeStamp, this.executionLenghth);
		innerCache.insertOneEntry(innerEntry);
		while ( innerEntry != null && (innerEntry.timeStamp.before(endingTime) )) {
			this.currentSystemReadTimeStamp = innerEntry.timeStamp;
			if( innerCache.store.size()>0 ) {
				int numOfJoinResults = innerCache.performJoin(innerEntry, innerCache, joinType, this.currentSystemReadTimeStamp);
				if(numOfJoinResults>0) {
					outputCounter += numOfJoinResults;
				}
			}
			innerEntry = innerCache.next(simTimeStamp, joinType);
			simTimeStamp++;
		}
		innerCache.endOfCache();
		
		Files.append(outputCounter+"\n", overallResults, Charsets.UTF_8);
		System.out.println("End of one way join; Num of results "+outputCounter);
	}
	public void warmupReset(){
		if(this.isWarmingUp == true && this.simTimeStamp>=this.numOfWarmUp) {
			//System.out.println(innerCache.currentLocalSimTime);
			//System.out.println(outerCache.currentLocalSimTime);
			if(this.innerCache.getClass().equals(DataCacheFIFOClock.class) || this.innerCache.getClass().equals(DataCacheFIFOLRU.class) ) {
				((DataCacheFIFOClock) this.innerCache).warmupReset();
				((DataCacheFIFOClock) this.outerCache).warmupReset();
			} 
			outputCounter 	= 0;
			isWarmingUp 	= false;
		}
		
	}
	public void twoWayJoinSim() throws Exception{
		//System.out.println("Start two way Join");
		DataEntry innerEntry 	= innerCache.next(simTimeStamp++, joinType);
		DataEntry outterEntry 	= outerCache.next(simTimeStamp++, joinType);
		endingTime = DateUtils.addDays(innerEntry.timeStamp, this.executionLenghth);
		int numOfJoinResults;
		
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(endingTime) || outterEntry.timeStamp.before(endingTime)) ) {
			// set the join direction, left join right or right join left
			Date tempCurrentTimeStamp = null;
			DataCache tempJoinCache = null;
			DataCache tempInputCache = null;
			DataEntry tempEntry = null;
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				tempCurrentTimeStamp = innerEntry.timeStamp;
				tempJoinCache 	= outerCache;
				tempEntry 		= innerEntry;
				tempInputCache	= innerCache;
				//numOfJoinResults = outerCache.performJoin(innerEntry, innerCache, joinType);
				innerEntry = innerCache.next(simTimeStamp, joinType);
			} else {
				tempCurrentTimeStamp = outterEntry.timeStamp;
				tempJoinCache 	= innerCache;
				tempEntry 		= outterEntry;
				tempInputCache	= outerCache;
				//numOfJoinResults = innerCache.performJoin(outterEntry, outerCache, joinType);
				outterEntry = outerCache.next(simTimeStamp, joinType);
			}
			simTimeStamp++;
			this.currentSystemReadTimeStamp = tempCurrentTimeStamp;
			numOfJoinResults = tempJoinCache.performJoin(tempEntry, tempInputCache, joinType, this.currentSystemReadTimeStamp);
			
			//System.out.println(Debug.sdf.format(this.currentSystemReadTimeStamp).toString());
			outputCounter +=numOfJoinResults;
			this.warmupReset();
		}
		//System.out.println(innerCache.currentLocalSimTime);
		//System.out.println(outerCache.currentLocalSimTime);
		
		innerCache.endOfCache();
		outerCache.endOfCache();
		Files.append(outputCounter+"\n", overallResults, Charsets.UTF_8);
		
		//System.out.println("End of two way join");
		if(this.innerCache.getClass().equals(DataCacheFIFOClock.class) || this.innerCache.getClass().equals(DataCacheFIFOLRU.class) ) {
			System.out.println(outputCounter+"\t"+((DataCacheFIFOClock) this.innerCache).printStat()+"\t"+((DataCacheFIFOClock) this.outerCache).printStat());
		} else
			System.out.println(outputCounter);
	
	}
	public void printInputWriteSimTimeStamp() throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DataEntry innerEntry = innerCache.next(++simTimeStamp, joinType);
		File file = new File("output//epg_sim_time.csv");
	    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
		output.write(simTimeStamp+"\t"+sdf.format(innerEntry.timeStamp).toString()+"\t"+sdf.format(innerEntry.timeStampEnd).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields+"\n");
	    
	    DataEntry outterEntry = outerCache.next(++simTimeStamp, joinType);
		File file1 = new File("output//log_sim_time.csv");
	    BufferedWriter output1 = new BufferedWriter(new FileWriter(file1, true));
		output1.write(simTimeStamp+"\t"+sdf.format(outterEntry.timeStamp).toString()+"\t"+sdf.format(outterEntry.timeStampEnd).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields+"\n");
	    
		endingTime = DateUtils.addDays(innerEntry.timeStamp, executionLenghth);
		
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(endingTime) || outterEntry.timeStamp.before(endingTime))) {
			simTimeStamp++;
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				innerEntry = innerCache.next(simTimeStamp, joinType);
				if(innerEntry != null)
					output.write(simTimeStamp+"\t"+sdf.format(innerEntry.timeStamp).toString()+"\t"+sdf.format(innerEntry.timeStampEnd).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields+"\n");
			} else {
				outterEntry = outerCache.next(simTimeStamp, joinType);
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
}

