package load_shedding_sim;
import java.util.*;
import java.io.*;
import java.text.*;

import com.google.common.base.Charsets;
import com.google.common.collect.*;
import com.google.common.io.*;

public class DataCache {
	BufferedReader fileBufferReader; // for input file
	
	File fileOutputStat;
	File fileOutputResults;
	
	public int allowedSize;
	boolean isInner;
	String outputFileNameBase;
	String outputDir;
	
	HashMultimap <String,DataEntry> store;
	boolean enableReasoning;
	PriorityQueue<DataEntry> endingTimeQ ;
	int currentLocalSimTime;
	Date currentRealTimeStamp;
	
	int numOfEvication;
	int numOfExpiried;
	
	public void warmupReset () {
		numOfEvication 	= 0;
		numOfExpiried	= 0;
	}
	public String printStat() {
		return this.numOfEvication +"\t"+ this.numOfExpiried;
	}
	public void putintoStore(DataEntry input){
		store.put(input.key, input);
		if(this.enableReasoning)
			endingTimeQ.offer(input);
	}
	public void deleteFromStore(DataEntry input){
		store.remove(input.key, input);
		if(this.enableReasoning)
			endingTimeQ.remove(input);
	}
	
	public void initOutPutFiles() throws Exception {
		fileOutputStat = new File(outputDir+outputFileNameBase+".txt");
		fileOutputResults = new File(outputDir+outputFileNameBase+"_resutls"+".txt");
	}
	
	public DataCache ( String inputFileDir, int allowedSize, boolean isInner, boolean enableReasoning, String outputFileNameBase, String outputDir ) throws Exception {
		//fileInput 	 = new File(inputFileDir);
		fileBufferReader = new BufferedReader( new FileReader(inputFileDir));
		this.allowedSize = allowedSize;
		store 		 = HashMultimap.create();
		this.isInner = isInner;
		this.outputDir= outputDir;
		
		this.outputFileNameBase = outputFileNameBase;
		initOutPutFiles();
		
		this.endingTimeQ = new PriorityQueue<DataEntry>(allowedSize, new Comparator<DataEntry>(){
	                public int compare(DataEntry a, DataEntry b){
	                    if (a.timeStampEnd.before(b.timeStampEnd) ) return -1;
	                    if (a.timeStampEnd.after(b.timeStampEnd) ) return 1;
	                    return 0;
	                }
	            });
		this.enableReasoning = enableReasoning;
	}
	// TO overload by subclass
	public DataEntry evicatOneEntry () throws Exception {return null;}
	public void insertOneEntry ( DataEntry newEntry) {}
	
	public DataEntry next(int simTimeStamp, int joinType) throws Exception {
		String inputStrig;
		if ((inputStrig = fileBufferReader.readLine()) != null) {
			DataEntry newEntry = joinType<20?new DataEntrySRBench (inputStrig, simTimeStamp):new DataEntryVistaTV (inputStrig, simTimeStamp);
			this.currentLocalSimTime 	= newEntry.localSimTimeStamp;
			this.currentRealTimeStamp 	= newEntry.timeStamp;
			return newEntry;
		} else {
			fileBufferReader.close();
			return null;
		}
	}
	public String convertDateFormate(Date input) {
		return Debug.sdf.format(input).toString();
	}

	public void printJoinResutls (DataEntry entry, DataEntry inputEntry,  DataCache inputCache) throws Exception {
		//try {
			if(this.isInner) {
				//Files.append(inputEntry.simTimeStamp+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.key+","+entry.otherDataFields+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.otherDataFields+"\n", fileOutputResults, Charsets.UTF_8);
				//Files.append(this.currentLocalSimTime+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.key+","+entry.otherDataFields+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.otherDataFields+","+entry.localSimTimeStamp+","+this.currentLocalSimTime+","+inputEntry.localSimTimeStamp+","+inputCache.currentLocalSimTime+"\n", fileOutputResults, Charsets.UTF_8);
				System.out.println(this.currentLocalSimTime+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.key+","+entry.otherDataFields+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.otherDataFields+","+entry.localSimTimeStamp+","+this.currentLocalSimTime+","+inputEntry.localSimTimeStamp+","+inputCache.currentLocalSimTime+"\n");
			} else {
				//Files.append(this.currentLocalSimTime+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.key+","+inputEntry.otherDataFields+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.otherDataFields+","+inputEntry.localSimTimeStamp+","+inputCache.currentLocalSimTime+","+entry.localSimTimeStamp+","+this.currentLocalSimTime+"\n", fileOutputResults, Charsets.UTF_8);
				System.out.println(this.currentLocalSimTime+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.key+","+inputEntry.otherDataFields+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.otherDataFields+","+inputEntry.localSimTimeStamp+","+inputCache.currentLocalSimTime+","+entry.localSimTimeStamp+","+this.currentLocalSimTime+"\n");
				
			}
			//}
		/* 
		catch ( IOException e ) {
			e.printStackTrace();
		}
		*/
			
	}
	
	public int performJoin(DataEntry inputEntry, DataCache inputCache, int joinType, Date currentSystemReadTimeStamp) throws Exception {
		this.currentRealTimeStamp = inputEntry.timeStamp;
		int numResults = 0;
		switch (joinType) {
			case Debug.JOIN_TYPE_ONE_WAY:
				if(Integer.parseInt(inputEntry.key) >= 10) {
					Set<DataEntry> entries = store.get(inputEntry.key);
					inputEntry.afterJoin(entries.size());
					for(DataEntry entry:entries) {
						if((Math.abs(entry.timeStampEnd.getTime() - inputEntry.timeStamp.getTime()) < 200)&& !entry.otherDataFields.equals(inputEntry.otherDataFields))
						{
							//printJoinResutls(entry, inputEntry);
							entry.afterJoin(1);
							numResults++;
						}
					}
					inputEntry.afterJoin(numResults);
				}
				break;
			case Debug.JOIN_TYPE_TWO_WAY:
			case 22:
				Set<DataEntry> entries = store.get(inputEntry.key);
				if(!this.getClass().equals(DataCacheClock.class) && !this.getClass().equals(DataCacheCLOCKONE.class))
					inputEntry.afterJoin(entries.size());
				for(DataEntry entry:entries) {
					if(!(entry.timeStampEnd.before(inputEntry.timeStamp)) && !(entry.timeStamp.after(inputEntry.timeStampEnd))) {
						//printJoinResutls(entry, inputEntry, inputCache);
						entry.afterJoin(1);
						if(this.getClass().equals(DataCacheFIFOClock.class) || this.getClass().equals(DataCacheFIFOLRU.class) ) {
							((DataCacheFIFOClock) this).hitInCache(entry);
							((DataCacheFIFOClock) inputCache).numOfTotalFIFOResults++;
						}else if(this.getClass().equals(DataCacheTRUELRU.class)){
							((DataCacheTRUELRU) this).index.get(entry.hashCode());
						}

						numResults++;
					}
					
				}
				break;
		}
		inputCache.garbageCollection(currentSystemReadTimeStamp);
		inputCache.insertOneEntry(inputEntry);
		inputCache.evicatOneEntry();
		return numResults;
	}
	
	public void endOfCache() throws Exception {
		//bufferOutputResults.flush();
		//bufferOutputResults.close();
	}
	public void garbageCollection ( Date currentSystemReadTimeStamp) {
	}
}