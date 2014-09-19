package load_shedding_sim;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;

import org.apache.commons.cli.*;
//import org.apache.commons.cli.Parser;
import org.apache.commons.lang3.time.DateUtils;

import strategy.DataCache;
import strategy.DataCacheARC;
import strategy.DataCacheCLOCKM;
import strategy.DataCacheCLOCKONE;
import strategy.DataCacheClock;
import strategy.DataCacheFIFO;
import strategy.DataCacheFIFOClock;
import strategy.DataCacheFIFOFIFO;
import strategy.DataCacheFIFOLRU;
import strategy.DataCacheKhoa;
import strategy.DataCacheRandom;
import strategy.DataCacheTRUELRU;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import data_entry.DataEntry;

public class JoinOperation {

	public DataCache innerCache;
	public DataCache outerCache;
	
	public int outputCounter;
	public int executionLenghth;
	public Date endingTime;
	public int globalSimTimeStamp;
	
	static CommandLine inputArguments;
	static CommandLineParser parser = new GnuParser();
	static Options options = new Options();
	
	static String outputDir;
	
	static File overallResults;
	int joinType;
	
	int numOfWarmUp;
	boolean isWarmingUp;
	
	public Date currentSystemRealTimeStamp;
	
	public Date nextSlotTimeStamp;
	public int currentSlotInnerInput;
	public int currentSlotOutterInput;
	public int currentSlotResutls;
	public int slotSize = 10;
	
	int statOfStressedInput;
	int statOfStressedResults;
	//int statOfStressedEvication;
	//int statOfStressedExpiried;
	int startingHourOfStressedSystem= 12;
	int endingHourOfStressedSystem	= 14;
	
	
	
	/*
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
		
		public TimeLineStat( int sizeOfSlot ){
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
	
	-T 21 -I //Users//shengao//git//loadshedding//loadshedding//input//epg_sim_time_local.csv -O //Users//shengao//git//loadshedding//loadshedding//input//log_with_oracle_3days.csv -OUT //Users//shengao//git//loadshedding//loadshedding//output// -Si 0.01 -So 0.5 -L 3 -CLOCK -SF 0.9 -W 600000 -TH 0
	
	*/
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
		options.addOption( "RANDOM", "random", false, "random evicate one");
		options.addOption( "CLOCKONE", "clock 1", false, "using clock 1 to simulate lru");
		options.addOption( "TRUELRU", "turelru", false, "not implemented as clock");
		options.addOption( "FIFO", "fifo", false, "fifo experiments");
		options.addOption( "CLOCK", "clock", false, "clock experiments");
		options.addOption( "CLOCKM", "clockm", false, "clock in the middle");
		options.addOption( "FIFOCLOCK", "fifoclock", false, "fifo+clock experiments");
		options.addOption( "FIFOLRU", "fifolru", false, "fifo+lru experiments");
		options.addOption( "FIFOFIFO", "fifofifo", false, "fifo+fifos experiments");
		options.addOption( "ARC", "arc", false, "IBM ARC experiment");
		options.addOption( "SF", "sizeOfFIFO", true, "fifo area size");
		options.addOption( "TH", "threshold", true, "fifo to other threshold, number of past results");
		
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
		
		double innerRatio;
		double outterRatio;
		
		switch(joinType) {
		case Debug.JOIN_TYPE_ONE_WAY:
			innerRatio = Double.parseDouble(inputArguments.getOptionValue("innerSize") );
			innerSize = innerRatio<=1?(int)(innerRatio*(double)(Debug.WINDSPEED_MAX_SIZE)):(int)innerRatio;
			outterRatio= Double.parseDouble(inputArguments.getOptionValue("onnerSize") );
			outterSize = outterRatio<=1?(int)(outterRatio*(double)(Debug.WINDSPEED_MAX_SIZE)):(int)outterRatio;
			break;
		case Debug.JOIN_TYPE_TWO_WAY:
			innerRatio = Double.parseDouble(inputArguments.getOptionValue("innerSize") );
			innerSize = innerRatio<=1?(int)(innerRatio*(double)(Debug.EPG_MAX_SIZE)):(int)innerRatio;
			outterRatio= Double.parseDouble(inputArguments.getOptionValue("onnerSize") );
			outterSize = outterRatio<=1?(int)(outterRatio*(double)(Debug.LOG_MAX_SIZE)):(int)outterRatio;
			break;
		}
		
		
		outputDir 	= inputArguments.getOptionValue("outputDir");
		outputDir 	= outputDir.concat(String.valueOf((new java.util.Date()).getTime())+"//");
		new File(outputDir).mkdir();
		
		this.executionLenghth 	= Integer.parseInt(inputArguments.getOptionValue("length") );
				
		if( inputArguments.hasOption( "ORACLE" ) ) {
			type = "ORACLE";
			oracleDepth = Integer.parseInt(inputArguments.getOptionValue("ORACLE") );
		} else if (inputArguments.hasOption( "RANDOM" ) ) {
			type = "RANDOM";
		} else if (inputArguments.hasOption( "FIFO" ) ) {
			type = "FIFO";
		} else if (inputArguments.hasOption( "CLOCKONE" ) ) {
			type = "CLOCKONE";
		} else if (inputArguments.hasOption( "TRUELRU" ) ) {
			type = "TRUELRU";
		} else if (inputArguments.hasOption( "CLOCK" ) ) {
			type = "CLOCK";
		} else if (inputArguments.hasOption( "CLOCKM" ) ) {
			type = "CLOCKM";
		} else if (inputArguments.hasOption( "FIFOCLOCK" ) ) {
			type = "FIFOCLOCK";
		} else if (inputArguments.hasOption( "FIFOLRU" ) ) {
			type = "FIFOLRU";
		} else if (inputArguments.hasOption( "FIFOFIFO" ) ) {
			type = "FIFOFIFO";
		} else if (inputArguments.hasOption( "ARC" ) ) {
			type = "ARC";
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
		} else if (type.equals( "RANDOM" ) ) {
			innerCache = new DataCacheRandom (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheRandom (outterDir, outterSize, false, enableReasoning, outputDir);
		} else if (type.equals( "TRUELRU" ) ) {
			innerCache = new DataCacheTRUELRU (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheTRUELRU (outterDir, outterSize, false, enableReasoning, outputDir);
		} else if (type.equals( "CLOCKONE" ) ) {
			innerCache = new DataCacheCLOCKONE (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheCLOCKONE (outterDir, outterSize, false, enableReasoning, outputDir);
		} else if (type.equals( "CLOCK" ) ) {
			innerCache = new DataCacheClock (innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheClock (outterDir, outterSize, false, enableReasoning, outputDir);
		}else if (type.equals( "CLOCKM" ) ) {
			innerCache = new DataCacheCLOCKM(innerDir, innerSize, true, enableReasoning, outputDir);
			outerCache = outterDir==null?null:new DataCacheCLOCKM (outterDir, outterSize, false, enableReasoning, outputDir);
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
		}else if (type.equals( "ARC" ) ) {
			// IBM adaptive replacement cache
			double sizeFIFO = Double.parseDouble(inputArguments.getOptionValue("sizeOfFIFO"));
			innerCache = new DataCacheARC (innerDir, innerSize, true, enableReasoning, outputDir, innerSize+outterSize);
			outerCache = outterDir==null?null:new DataCacheARC(outterDir, outterSize, false, enableReasoning, outputDir, innerSize+outterSize);
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
		//System.out.println("Start one way Join ");
		Date tempCurrentTimeStamp = null;
		DataEntry innerEntry = innerCache.next(globalSimTimeStamp++, joinType);
		endingTime = DateUtils.addDays(innerEntry.timeStamp, this.executionLenghth);
		innerCache.insertOneEntry(innerEntry);
		nextSlotTimeStamp = DateUtils.addSeconds(innerEntry.timeStamp, slotSize);
		
		while ( innerEntry != null && (innerEntry.timeStamp.before(endingTime) )) {
			tempCurrentTimeStamp = innerEntry.timeStamp;
			this.currentSystemRealTimeStamp = innerEntry.timeStamp;
			if( innerCache.store.size()>0 ) {
				int numOfJoinResults = innerCache.performJoin(innerEntry, innerCache, joinType, this.currentSystemRealTimeStamp);
				
				outputCounter += numOfJoinResults;
				// stat
				this.currentSlotInnerInput++;
				this.currentSlotResutls+=numOfJoinResults;
				
				// stressed system statistics
				Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
				calendar.setTime(this.currentSystemRealTimeStamp);
				//System.out.println(calendar.getTime().toString());
				int currentSec = calendar.get(Calendar.SECOND);
				//System.out.println(currentHour);
				if(currentSec<=50 && currentSec >=40) {
					//System.out.println( ");
					this.statOfStressedInput	++;
					this.statOfStressedResults 	+= numOfJoinResults;
				}
			}
			
			innerEntry = innerCache.next(globalSimTimeStamp, joinType);
			globalSimTimeStamp++;
			
			/*
			this.currentSystemRealTimeStamp = tempCurrentTimeStamp;
			//System.out.println(Debug.sdf.format(this.currentSystemRealTimeStamp).toString());
			
			if(nextSlotTimeStamp.before(this.currentSystemRealTimeStamp)) {
				nextSlotTimeStamp = DateUtils.addSeconds(this.currentSystemRealTimeStamp, slotSize);
				System.out.println(Debug.sdf.format(this.currentSystemRealTimeStamp).toString()+"\t"+this.currentSlotInnerInput +"\t"+this.currentSlotResutls +"\t"+this.innerCache.store.size());
				this.currentSlotInnerInput 	= 0;
				this.currentSlotResutls 	= 0;
			}
			*/
			
			
		}
		innerCache.endOfCache();
		
		Files.append(outputCounter+"\n", overallResults, Charsets.UTF_8);
		//System.out.println(outputCounter+"\t"+ this.innerCache.printStat()+"\t"+this.outerCache.printStat());
		this.printStat();
	}
	public void warmupReset(){
		if(this.isWarmingUp == true && this.globalSimTimeStamp>=this.numOfWarmUp) {
			this.innerCache.warmupReset();
			this.outerCache.warmupReset();
			outputCounter 	= 0;
			isWarmingUp 	= false;
			this.statOfStressedInput	= 0;
			this.statOfStressedResults 	= 0;

		}
		
	}
	public void printStat(){
		System.out.println(outputCounter+"\t"+this.statOfStressedInput+"\t"+this.statOfStressedResults+"\t"+ this.innerCache.printStat()+"\t"+this.outerCache.printStat());
	}
	public void twoWayJoinSim() throws Exception{
		DataEntry innerEntry 	= innerCache.next(globalSimTimeStamp++, joinType);
		DataEntry outterEntry 	= outerCache.next(globalSimTimeStamp++, joinType);
		endingTime = DateUtils.addDays(innerEntry.timeStamp, this.executionLenghth);
		int numOfJoinResults;
		
		
		// temp stat
		//int largestActiveEPG = 0;
		//nextSlotTimeStamp = DateUtils.addHours(innerEntry.timeStamp, slotSize);
			int numberOfInnerEntry= 0;
			int numberOfOutterEntry= 0;
			
			
			
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(endingTime) || outterEntry.timeStamp.before(endingTime)) ) {
			// set the join direction, left join right or right join left
			long startNano = System.nanoTime();
			
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
				innerEntry = innerCache.next(globalSimTimeStamp, joinType);
				this.currentSlotInnerInput++;
				numberOfInnerEntry++;
			} else {
				tempCurrentTimeStamp = outterEntry.timeStamp;
				tempJoinCache 	= innerCache;
				tempEntry 		= outterEntry;
				tempInputCache	= outerCache;
				//numOfJoinResults = innerCache.performJoin(outterEntry, outerCache, joinType);
				outterEntry = outerCache.next(globalSimTimeStamp, joinType);
				this.currentSlotOutterInput++;
				numberOfOutterEntry++;
			}
			globalSimTimeStamp++;
			this.currentSystemRealTimeStamp = tempCurrentTimeStamp;
			numOfJoinResults = tempJoinCache.performJoin(tempEntry, tempInputCache, joinType, this.currentSystemRealTimeStamp);
			
			// statistics
			outputCounter +=numOfJoinResults;
			// stressed system statistics
			Calendar calendar = GregorianCalendar.getInstance(); // creates a new calendar instance
			calendar.setTime(this.currentSystemRealTimeStamp);
			//System.out.println(calendar.getTime().toString());
			int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
			//System.out.println(currentHour);
			//this.statOfStressedInput++;
			if(currentHour<=this.endingHourOfStressedSystem && currentHour >=this.startingHourOfStressedSystem) {
				//System.out.println( ");
				this.statOfStressedInput	++;
				this.statOfStressedResults 	+= numOfJoinResults;
			}
			
			
			
			
			this.warmupReset();
			//this.innerCache.myDeprecation.calculateDepreciate(this.globalSimTimeStamp, this.numOfWarmUp*2/3);
			//this.outerCache.myDeprecation.calculateDepreciate(this.globalSimTimeStamp, this.numOfWarmUp*2/3);
			this.innerCache.myDeprecation.calculateDepreciate(this.globalSimTimeStamp, 10000);
			//this.outerCache.myDeprecation.calculateDepreciate(this.globalSimTimeStamp, 100);
			
			// temp stat
			/*			
			if(largestActiveEPG <  this.innerCache.store.size()) {
				largestActiveEPG = this.innerCache.store.size();
				System.out.println(largestActiveEPG);
			}
			this.currentSlotResutls+=numOfJoinResults;
			if(nextSlotTimeStamp.before(this.currentSystemRealTimeStamp)) {
				nextSlotTimeStamp = DateUtils.addHours(this.currentSystemRealTimeStamp, slotSize);
				System.out.println(Debug.sdf.format(this.currentSystemRealTimeStamp).toString()+"\t"+this.currentSlotInnerInput +"\t"+this.currentSlotOutterInput +"\t"+this.currentSlotResutls+"\t"+this.innerCache.store.size()+"\t"+this.outerCache.store.size() );
				this.currentSlotInnerInput 	= 0;
				this.currentSlotResutls 	= 0;
				this.currentSlotOutterInput = 0;
				
			}*/
			startNano = System.nanoTime() - startNano;
		}
		//System.out.println(innerCache.currentLocalSimTime);
		//System.out.println(outerCache.currentLocalSimTime);
		
		innerCache.endOfCache();
		outerCache.endOfCache();
		Files.append(outputCounter+"\n", overallResults, Charsets.UTF_8);
		
		this.printStat();
		//System.out.println(outputCounter+"\t"+this.statOfStressedInput+"\t"+this.statOfStressedResults+"\t"+ this.innerCache.printStat()+"\t"+this.outerCache.printStat());
		/*
		if(this.innerCache.getClass().equals(DataCacheFIFOClock.class) || this.innerCache.getClass().equals(DataCacheFIFOLRU.class) ) {
			System.out.println(outputCounter+"\t"+((DataCacheFIFOClock) this.innerCache).printStat()+"\t"+((DataCacheFIFOClock) this.outerCache).printStat());
		} else
			System.out.println(outputCounter);
		 */
	}
	public void printInputWriteSimTimeStamp() throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DataEntry innerEntry = innerCache.next(++globalSimTimeStamp, joinType);
		File file = new File("output//epg_sim_time.csv");
	    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
		output.write(globalSimTimeStamp+"\t"+sdf.format(innerEntry.timeStamp).toString()+"\t"+sdf.format(innerEntry.timeStampEnd).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields+"\n");
	    
	    DataEntry outterEntry = outerCache.next(++globalSimTimeStamp, joinType);
		File file1 = new File("output//log_sim_time.csv");
	    BufferedWriter output1 = new BufferedWriter(new FileWriter(file1, true));
		output1.write(globalSimTimeStamp+"\t"+sdf.format(outterEntry.timeStamp).toString()+"\t"+sdf.format(outterEntry.timeStampEnd).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields+"\n");
	    
		endingTime = DateUtils.addDays(innerEntry.timeStamp, executionLenghth);
		
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(endingTime) || outterEntry.timeStamp.before(endingTime))) {
			globalSimTimeStamp++;
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				innerEntry = innerCache.next(globalSimTimeStamp, joinType);
				if(innerEntry != null)
					output.write(globalSimTimeStamp+"\t"+sdf.format(innerEntry.timeStamp).toString()+"\t"+sdf.format(innerEntry.timeStampEnd).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields+"\n");
			} else {
				outterEntry = outerCache.next(globalSimTimeStamp, joinType);
				if(outterEntry != null)
					output1.write(globalSimTimeStamp+"\t"+sdf.format(outterEntry.timeStamp).toString()+"\t"+sdf.format(outterEntry.timeStampEnd).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields+"\n");
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

