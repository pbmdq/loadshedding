package strategies;
import java.util.*;

import data_entry.DataEntry;

public class DataCacheClock extends DataCache{
	LinkedList <DataEntry> index;
	int pointer;
	
	public DataCacheClock ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, "CLOCK", outputDir);
		index = new LinkedList<DataEntry> (); 
		pointer = 0;
	}
	
	public DataEntry replaceVictimEntry ( DataEntry input) {
		DataEntry tempData;
		ListIterator <DataEntry> tempItr = index.listIterator(pointer);
		for(;;)
		{
			if(!tempItr.hasNext()) 
				tempItr = index.listIterator(0);
			tempData = tempItr.next();
			if(tempData.numberOfPastResults>0) {
				tempData.numberOfPastResults--;
			}
			else
				break;
		}
		this.statOfTotalEvication++;
		tempItr.set(input);
		this.deleteFromStore(tempData);
		index.remove(tempData);
		//store.remove(tempData.key, tempData);
		pointer = tempItr.nextIndex();
		return tempData;
	}
	
	public DataEntry evicatOneEntry () {
		return null;
	}
	
	public void insertOneEntry ( DataEntry newEntry) {
		if( store.size() >= allowedSize ) {
			replaceVictimEntry(newEntry);
			index.add(newEntry);
		} else
			index.add(newEntry);
		this.putintoStore(newEntry);
	}
	public void garbageCollection (Date currentSystemReadTimeStamp) {
		if(this.enableReasoning) {
			DataEntry temEntry = this.endingTimeQ.peek();
			while (temEntry != null && temEntry.timeStampEnd.before(currentSystemReadTimeStamp)) {
				this.deleteFromStore(temEntry);
				this.statOfTotalExpiried++;
				index.remove(temEntry);
				temEntry = this.endingTimeQ.peek();
			}
		}
	}
}