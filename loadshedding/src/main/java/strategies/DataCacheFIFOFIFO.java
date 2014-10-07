package strategies;
import java.util.*;

import load_shedding_sim.Debug;
import data_entry.DataEntry;

public class DataCacheFIFOFIFO extends DataCache{
	int []sizeFF;
	int []numOfThrowingAway;
	int []numOfTotalKeeping;
	int []numOfTotalResults;
	
	int thresHold;
	
	Queue <DataEntry> []indexFIFO;
	
	public void warmupReset () {
		for(int i = 0; i<Debug.FF_NUM_SEG; i++) {
			numOfThrowingAway[i] = 0;
			numOfTotalKeeping[i] = 0;
			numOfTotalResults[i] = 0;
		}
		
	}
	public DataCacheFIFOFIFO ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir, double sizeFIFO, int thresHold) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, "FIFOCLOCK", outputDir);
		indexFIFO 	=  new LinkedList [Debug.FF_NUM_SEG] ; 
		for(int i = 0; i<Debug.FF_NUM_SEG; i++) {
			indexFIFO[i] = new LinkedList <DataEntry>();
		}
		sizeFF 	= new int [Debug.FF_NUM_SEG];
		numOfThrowingAway = new int [Debug.FF_NUM_SEG];
		numOfTotalKeeping = new int [Debug.FF_NUM_SEG];
		numOfTotalResults = new int [Debug.FF_NUM_SEG];
		sizeFF[0] = 15;
		sizeFF[1] = 10;
		sizeFF[2] = 90;
		
//		sizeFF[0] = 3;
//		sizeFF[1] = 3;
//		sizeFF[2] = 3;
		warmupReset ();
		
		this.thresHold = thresHold;
	}
	public String printStat() {
		String output = null;
		for(int i = 0; i<Debug.FF_NUM_SEG; i++) {
			output.concat(numOfTotalKeeping[i]+"\t");
			output.concat(numOfThrowingAway[i]+"\t");
		}
		return output;
	}
	
	public boolean willThrowAway( DataEntry temEntry ){
		if(temEntry.numberOfPastResults>=this.thresHold) {
			return false;
		} else {
			return true;
		}
	}
	public void hitInCache(DataEntry temEntry){
		numOfTotalResults[temEntry.segID]++;
	}
	public int totalSize(){
		int totalSize = 0;
		for(int i = 0; i<Debug.FF_NUM_SEG; i++) {
			totalSize += indexFIFO[i].size();
		}
		return totalSize;
	}
	
	public DataEntry removeEntry(int segID , DataEntry temEntry){
		numOfThrowingAway[segID]++;
		store.remove(temEntry.key, temEntry);
		return temEntry;
	}
	public void keepEntry(int segID , DataEntry temEntry){
		numOfTotalKeeping[segID]++;
		temEntry.beforeSwitchNextSeg(segID+1);
		indexFIFO[segID+1].offer(temEntry);
		
	}
	
	public DataEntry evicatOneEntry () throws Exception {
		DataEntry temEntry = null;
		int i = 0;	
		// warm up case
		//System.out.println(totalSize());
		if(totalSize()<this.allowedSize) {
		//if(totalSize()<=9) {
				
			while(i<Debug.FF_NUM_SEG-1) {
				if(indexFIFO[i].size()>sizeFF[i])  {
					temEntry = indexFIFO[i].poll();	
					keepEntry(i, temEntry);
				}
				i++;
			}
		} else {
			i = 0;
			while(i<Debug.FF_NUM_SEG-1) {
				if(indexFIFO[i].size()>sizeFF[i]) {
					temEntry = indexFIFO[i].poll();	
					if(!willThrowAway(temEntry)){
						keepEntry(i, temEntry );
					}else {
						return removeEntry(i, temEntry );
						}
					
					}
					i++;
				}
			
			if(indexFIFO[i].size()>sizeFF[i]) {
				temEntry = indexFIFO[i].poll();	
				return removeEntry(i, temEntry );
			}
		}
		return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		indexFIFO[0].add(newEntry);
		store.put(newEntry.key, newEntry);
	}
}