package strategies;
import java.util.*;

import data_entry.DataEntry;

public class DataCacheFIFOClock extends DataCache{
	LinkedList <DataEntry> indexCLOCK;
	int sizeFIFO;
	int sizeCLOCK;
	double numOfTotalCLOCKResults;
	double numOfTotalCLOCKInput;
	
	double numOfTotalFIFOResults;
	double numOfTotalFIFOInput;
	
	int numOfThrowingAway;
	int numOfTotalKeeping;
	
	int thresHold;
	
	Queue <DataEntry> indexFIFO;
	int pointerCLOCK;
	
	public void warmupReset () {
		super.warmupReset();
		numOfTotalFIFOResults 	= 0;
		numOfTotalFIFOInput		= 0;
		numOfTotalCLOCKResults	= 0;
		numOfTotalCLOCKInput	= 0;
		numOfThrowingAway		= 0;
		numOfTotalKeeping		= 0;
		
	}
	public DataCacheFIFOClock ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir, double sizeFIFO, int thresHold) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, "FIFOCLOCK", outputDir);
		indexCLOCK 	= new LinkedList<DataEntry> (); 
		indexFIFO	= new LinkedList<DataEntry> (); 
		this.sizeFIFO= (int)(sizeFIFO*allowedSize);
		this.sizeCLOCK = allowedSize-this.sizeFIFO;
		pointerCLOCK = 0;
		this.thresHold	= thresHold;
		warmupReset ();
	}
	public String printStat() {
		return super.printStat()+"\t"+numOfTotalFIFOResults +"\t"+ numOfTotalCLOCKResults +"\t"+ numOfTotalKeeping+"\t"+ numOfThrowingAway;
	}
	
	public boolean willThrowAway( DataEntry temEntry ){
		if(temEntry.numberOfPastResults>=this.thresHold) {
			numOfTotalKeeping++;
			return false;
		} else {
			numOfThrowingAway++;
			return true;
		}
		
		//return false;
		/*
		if(this.numOfTotalCLOCKResults== 0 || (double)temEntry.numberOfPastResults/(double)this.sizeFIFO > this.numOfTotalCLOCKResults/this.numOfTotalCLOCKInput) {
			return false;
		} else
			return true;
		*/	
	}
	public void hitInCache(DataEntry temEntry){
		if(temEntry.isInFIFO)
			numOfTotalFIFOResults++;
		else
			numOfTotalCLOCKResults++;
	}
	
	public DataEntry evicatOneEntry () throws Exception {
		DataEntry temEntry = null;
		if(indexFIFO.size() > sizeFIFO)
			temEntry = indexFIFO.poll();
		if(temEntry!= null) {
			this.numOfTotalCLOCKInput++;
			
			if(indexCLOCK.size() < this.sizeCLOCK ) {
				temEntry.beforeSwitchToCLOCK();
				indexCLOCK.add(temEntry);
			} else {
				if (willThrowAway(temEntry)) {
					store.remove(temEntry.key, temEntry);
				} else {
					temEntry.beforeSwitchToCLOCK();
					temEntry = replaceVictimEntryCLOCK(temEntry);
					store.remove(temEntry.key, temEntry);
				}
				return temEntry;
				
			}
		}
		return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		indexFIFO.add(newEntry);
		store.put(newEntry.key, newEntry);
	}

	public DataEntry replaceVictimEntryCLOCK ( DataEntry input) {
		DataEntry tempData;
		ListIterator <DataEntry> tempItr = indexCLOCK.listIterator(this.pointerCLOCK);
		for(;;)
		{
			if(!tempItr.hasNext()) 
				tempItr = indexCLOCK.listIterator(0);
			tempData = tempItr.next();
			if(tempData.numberOfPastResults>0)
				tempData.numberOfPastResults--;
			else
				break;
		}
		tempItr.set(input);
		this.pointerCLOCK = tempItr.nextIndex();
		return tempData;
	}
}