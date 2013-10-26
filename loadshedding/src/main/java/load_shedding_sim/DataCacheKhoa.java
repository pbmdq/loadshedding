package load_shedding_sim;
import java.util.*;

public class DataCacheKhoa extends DataCache {
	PriorityQueue <DataEntry> index; // LRU queue
	
	public class numberOfTotalResultsCompare implements Comparator<DataEntry> {
	    public int compare(DataEntry x, DataEntry y) {
	        if (x.numberOfTotalResults < y.numberOfTotalResults) {
	            return -1;
	        } else if (x.numberOfTotalResults > y.numberOfTotalResults) {
	            return 1;
	        } else
	        	return 0;
	    }
	}
	public DataCacheKhoa ( String inputFileDir, int allowedSize, boolean isInner ) throws Exception {
		super( inputFileDir, allowedSize, isInner, "LRU");
		this.initOutPutFiles();
		Comparator<DataEntry> comparator = new numberOfTotalResultsCompare();
		index = new PriorityQueue<DataEntry> (allowedSize, comparator);  
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