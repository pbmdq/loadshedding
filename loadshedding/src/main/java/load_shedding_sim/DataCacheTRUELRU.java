package load_shedding_sim;
import java.util.*;
//import org.apache.commons.collections.*;

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
		store.put(newEntry.key, newEntry);
		DataEntry temp = null;
		temp = (DataEntry) index.put(newEntry.hashCode(), newEntry);
		if(temp!= null)
			store.remove(temp.key, temp);
		
	}
}