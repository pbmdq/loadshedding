package load_shedding_sim;
import java.util.*;

public class DataCacheLRU extends DataCache {
	Queue <DataEntry> index; // LRU queue
	
	public DataCacheLRU ( String inputFileDir, int allowedSize, boolean isInner ) throws Exception {
		super( inputFileDir, allowedSize, isInner, "LRU");
		this.initOutPutFiles();
		index = new LinkedList<DataEntry> (); 
	}
	
	public DataEntry evicatOneEntry () throws Exception {
		if( store.size() == allowedSize ) {
			DataEntry temEntry = index.poll();
			store.remove(temEntry.key, temEntry);
			return temEntry;
		} else
			return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		store.put(newEntry.key, newEntry);
		index.add(newEntry);
	}
}