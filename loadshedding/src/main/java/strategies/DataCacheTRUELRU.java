package strategies;
import java.util.*;

import data_entry.DataEntry;

public class DataCacheTRUELRU extends DataCache{
	
	LRUCache index; // LRU queue
	
	public DataCacheTRUELRU ( String inputFileDir, int allowedSize, boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, "TrueLRU", outputDir);
		index = new LRUCache(allowedSize);
	}

	public DataEntry evicatOneEntry () throws Exception {
			return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		this.putintoStore(newEntry);//store.put(newEntry.key, newEntry);
		DataEntry temp = null;
		temp = (DataEntry) index.put(newEntry.uniqueID, newEntry);
		if(temp!= null)
			this.deleteFromStore(temp);
			//store.remove(temp.key, temp);
	}
	public void garbageCollection (Date currentSystemReadTimeStamp) {
		if(this.enableReasoning) {
			DataEntry temEntry = this.endingTimeQ.peek();
			while (temEntry != null && temEntry.timeStampEnd.before(currentSystemReadTimeStamp)) {
				this.deleteFromStore(temEntry);
				this.statOfTotalExpiried++;
				index.remove(temEntry.uniqueID);
				temEntry = this.endingTimeQ.peek();
			}
		}
	}
}