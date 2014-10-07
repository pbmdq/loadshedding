package strategies;
import java.util.*;
import load_shedding_sim.Debug;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.HashMultimap;
import data_entry.DataEntry;

public class DataCacheARC extends DataCache{
	private static final Logger logger = LogManager.getLogger("ARC");
	
	LRUCache indexOne;
	LRUCache indexTwo;
	
	LRUCache indexShadowOne;
	LRUCache indexShadowTwo;
	
	HashMultimap <String,DataEntry> shadowStore;
	int targetSizeOne;
	int totalCacheSize;
	
	
	// stat fields
	double numOfTotalOneResults;
	double numOfTotalOneInput;
	
	double numOfTotalTwoResults;
	double numOfTotalTwoInput;
	
	int numOfThrowingAway;
	int numOfTotalKeeping;

	public void warmupReset () {
		super.warmupReset();
		numOfTotalOneResults 	= 0;
		numOfTotalOneInput		= 0;
		numOfTotalTwoResults	= 0;
		numOfTotalTwoInput		= 0;
		numOfThrowingAway		= 0;
		numOfTotalKeeping		= 0;
	}
	public DataCacheARC ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir, int totalCacheSize) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, "ARC", outputDir);
		indexOne 		= new LRUCache(allowedSize); 
		indexTwo		= new LRUCache(allowedSize);
		indexShadowOne 	= new LRUCache(allowedSize);
		indexShadowTwo 	= new LRUCache(allowedSize);
		this.targetSizeOne = allowedSize/2;
		this.shadowStore = HashMultimap.create();
		this.totalCacheSize = totalCacheSize;
		warmupReset ();
	}
	public String printStat() {
		//return super.printStat()+"\t"+numOfTotalFIFOResults +"\t"+ numOfTotalLRUResults +"\t"+ numOfTotalKeeping+"\t"+ numOfThrowingAway;
		return null;
	}
	public void hitInCache(DataEntry temEntry){
		int promoteThreshold = 2;
		if(temEntry.ARCQ == Debug.ARCQONE) {// test if promote to queue 2
			if(temEntry.ARCReference < promoteThreshold) {
				temEntry.ARCReference++;
				this.indexOne.get(temEntry.uniqueID);
				logger.debug(temEntry.key +" "+ temEntry.otherDataFields );
				//logger.debug("hit in cache 1: " + temEntry.);
			}else { // promote to queue 2
				assert temEntry.ARCReference == promoteThreshold;
				indexOne.remove(temEntry.uniqueID);
				temEntry.ARCQ = Debug.ARCQTWO;
				this.indexTwo.put(temEntry.uniqueID, temEntry);
				logger.debug("promote " + temEntry.key +" "+ temEntry.otherDataFields );
				logger.debug(this.indexOne.getSize() +" " + this.indexTwo.getSize() );
			}
		} else if (temEntry.ARCQ == Debug.ARCQTWO) { // hit in queue 2
			this.indexTwo.get(temEntry.uniqueID);
			logger.debug("hit q 2 " + temEntry.key +" "+ temEntry.otherDataFields );
		}
	}
	
	public DataEntry evicatOneEntry () throws Exception {
		return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		DataEntry victimEntry = null;
		LRUCache overSizedQueue = null;
		if (store.size() < this.allowedSize) {
			overSizedQueue = null;
		} else {
			assert (this.indexOne.getSize()+this.indexTwo.getSize() )== store.size();
			assert store.size() <= this.allowedSize ;
			if( this.indexOne.getSize() >= this.targetSizeOne) { // evict from q_1
				overSizedQueue = this.indexOne;
			} else {
				overSizedQueue = this.indexTwo;
			}
		}
		if(overSizedQueue != null)
			victimEntry = overSizedQueue.removeLastEntry();
		handleVictim(victimEntry);
		newEntry.ARCQ = Debug.ARCQONE;
		indexOne.put(newEntry.uniqueID, newEntry);
		store.put(newEntry.key, newEntry);
		if(this.enableReasoning)
			endingTimeQ.offer(newEntry);
		//if(this.isInner)
			//System.out.println( this.indexOne.getSize()+" "+this.indexShadowOne.getSize()+" "+this.indexShadowTwo.getSize()+" "+this.targetSizeOne);
	}
	// delete from store
	// put into shadow store
	public void handleVictim(DataEntry victimEntry) {
		if(victimEntry != null) {
			logger.debug(victimEntry.key +" "+ victimEntry.otherDataFields+" "+ victimEntry.ARCQ );
			LRUCache targetShadowQueue;
			if(victimEntry.ARCQ == Debug.ARCQONE) {
				targetShadowQueue = this.indexShadowOne;
			}else{
				targetShadowQueue = this.indexShadowTwo;
			}
			DataEntry tempShadowEntry = (DataEntry) targetShadowQueue.put(victimEntry.uniqueID, victimEntry);
			if(tempShadowEntry != null) {
				shadowStore.remove(tempShadowEntry.key, tempShadowEntry);
				if (this.enableReasoning)
					this.endingTimeQ.remove(tempShadowEntry);
				tempShadowEntry = null;
			}
			victimEntry.ARCQ = victimEntry.ARCQ*(-1);
			store.remove(victimEntry.key, victimEntry);
			shadowStore.put(victimEntry.key, victimEntry);
			//this.endingTimeQ.of
			//this.endingTimeQ.
			
		}
			
	}
	
	// check if there is a hit in the shadow queues
	// if there is, add the target size of the corresponding real queue
	public void shadowJoin(DataEntry inputEntry) throws Exception {
		Set<DataEntry> entries = this.shadowStore.get(inputEntry.key);
		int countForShadowOne = 0;
		int countForShadowTwo = 0;
		for(DataEntry entry:entries) {
			logger.debug(entry.key +" "+ entry.otherDataFields );
			if(entry.ARCQ == Debug.ARCQSHADOWONE) {// 
				this.indexShadowOne.get(inputEntry.uniqueID);
				countForShadowOne++;
			} else if (entry.ARCQ == Debug.ARCQSHADOWTWO) { // 
				countForShadowTwo++;
				this.indexShadowTwo.get(inputEntry.uniqueID);
			}
			
			if(countForShadowOne> 0 && countForShadowTwo == 0) {
				if(this.targetSizeOne >= this.allowedSize-1 )
					this.targetSizeOne = this.allowedSize-1;
				else
					this.targetSizeOne ++;
			}else if (countForShadowTwo> 0 && countForShadowOne == 0){
				if(this.targetSizeOne <= 1)
					this.targetSizeOne = 1;
				else
					this.targetSizeOne --;
			}
			
			//if(this.isInner && countForShadowOne !=0 && countForShadowTwo!= 0)
			//	System.out.println("indexOne: "+ countForShadowOne + "indexTwo: " + countForShadowTwo);
//			logger.info("target indexOne Size: "+this.targetSizeOne + "actual size: " + this.indexOne.getSize());
			
		}
	}
	public void garbageCollection (Date currentSystemReadTimeStamp) {
		if(this.enableReasoning) {
			DataEntry tempEntry = this.endingTimeQ.peek();
			while (tempEntry != null && tempEntry.timeStampEnd.before(currentSystemReadTimeStamp)) {
				if(tempEntry.ARCQ == Debug.ARCQONE) {// 
					this.indexOne.remove(tempEntry.uniqueID);
					this.store.remove(tempEntry.key, tempEntry);
				} else if (tempEntry.ARCQ == Debug.ARCQTWO) { // 
					this.indexTwo.remove(tempEntry.uniqueID);
					this.store.remove(tempEntry.key, tempEntry);
				} else	if(tempEntry.ARCQ == Debug.ARCQSHADOWONE) {
					this.indexShadowOne.remove(tempEntry.uniqueID);
					this.shadowStore.remove(tempEntry.key, tempEntry);
				} else if (tempEntry.ARCQ == Debug.ARCQSHADOWTWO) { // 
					this.indexShadowTwo.remove(tempEntry.uniqueID);
					this.shadowStore.remove(tempEntry.key, tempEntry);
				}
				//tempEntry = null;
				this.endingTimeQ.remove(tempEntry);
				tempEntry = this.endingTimeQ.peek();
				//if(this.isInner)
					//System.out.println( this.indexOne.getSize()+" "+this.indexShadowOne.getSize()+" "+this.indexShadowTwo.getSize()+" "+this.targetSizeOne);
			
			}
			
		}
	}
}