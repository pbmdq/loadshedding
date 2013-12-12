package load_shedding_sim;
import java.util.*;

public class DataCacheKhoa extends DataCache {
	PriorityQueue <DataEntry> index;
	int depthForsee;
	
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
	public DataCacheKhoa ( String inputFileDir, int allowedSize, boolean isInner, String outputDir,int depth) throws Exception {
		super( inputFileDir, allowedSize, isInner, "KHOA-"+depth, outputDir);
		this.depthForsee = depth;
		this.initOutPutFiles();
		
		
		
		Comparator<DataEntry> comparator = new numberOfTotalResultsCompare();
		index = new PriorityQueue<DataEntry> (allowedSize, comparator);  
	}
	public DataEntry next(int simTimeStamp, int joinType) throws Exception {
		String inputStrig;
		if ((inputStrig = fileBufferReader.readLine()) != null) {
			DataEntry newEntry = joinType<20?new DataEntrySRBench (inputStrig, simTimeStamp):new DataEntryVistaTV (inputStrig, simTimeStamp);
			return newEntry;
		} else {
			fileBufferReader.close();
			return null;
		}
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