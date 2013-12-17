package load_shedding_sim;
import java.util.*;

public class DataCacheRandom extends DataCacheClock {
	//LinkedList <DataEntry> index;
	//int pointer;
	
	public DataCacheRandom ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, outputDir);
		//index = new LinkedList<DataEntry> (); 
		//pointer = 0;
	}
	
	public DataEntry replaceVictimEntry ( DataEntry input) {
		DataEntry tempData = null;
		ListIterator <DataEntry> tempItr = index.listIterator(0);
		int evicatIndex = (int)(Math.random()*(index.size()));
		//evicatIndex = 56;
		//System.out.println(evicatIndex);
		for(int i = 0; i < evicatIndex-1; i++) {
			tempData = tempItr.next();
		}
		
		if(tempData != null) {
			this.numOfEvication++;
			tempItr.set(input);
			this.deleteFromStore(tempData);
			//store.remove(tempData.key, tempData);
			return tempData;
		}else
			return null;
	}
	
	/*public DataEntry evicatOneEntry () {
		return null;
	}
	
	public void insertOneEntry ( DataEntry newEntry) {
		if( store.size() == allowedSize ) {
			replaceVictimEntry(newEntry);
		} else
			index.add(newEntry);
		this.putintoStore(newEntry);
	}
	public void garbageCollection (Date currentSystemReadTimeStamp) {
		if(this.enableReasoning) {
			DataEntry temEntry = this.endingTimeQ.peek();
			//if(temEntry != null)
			//	System.out.println(Debug.sdf.format(currentSystemReadTimeStamp) +"\t"+ Debug.sdf.format(temEntry.timeStampEnd)+"\t"+ this.isInner);
			while (temEntry != null && temEntry.timeStampEnd.before(currentSystemReadTimeStamp)) {
				
//				System.out.println("deleting"+ Debug.sdf.format(currentSystemReadTimeStamp) +"\t"+ Debug.sdf.format(temEntry.timeStampEnd)+"\t"+ this.isInner);
//				System.out.println(this.allowedSize);
//				System.out.println(this.store.size());
//				System.out.println(this.index.size());
				
				this.deleteFromStore(temEntry);
				this.numOfExpiried++;
				index.remove(temEntry);
				temEntry = this.endingTimeQ.peek();
				
//				System.out.println(this.store.size());
//				System.out.println(this.index.size());
			}
		}
	}*/
}