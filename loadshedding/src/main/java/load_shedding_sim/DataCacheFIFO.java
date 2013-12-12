package load_shedding_sim;
import java.util.*;

public class DataCacheFIFO extends DataCache {
	Queue <DataEntry> index; // LRU queue
	
	public DataCacheFIFO ( String inputFileDir, int allowedSize, boolean isInner , String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, "LRU", outputDir);
		index = new LinkedList<DataEntry> (); 
	}

	public DataEntry evicatOneEntry () throws Exception {
		if( store.size() >= allowedSize ) {
			DataEntry temEntry = index.poll();
			store.remove(temEntry.key, temEntry);
			return temEntry;
		} else
			return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		store.put(newEntry.key, newEntry);
		index.offer(newEntry);
	}
}