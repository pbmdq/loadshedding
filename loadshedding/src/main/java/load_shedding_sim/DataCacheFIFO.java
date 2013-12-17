package load_shedding_sim;
import java.util.*;

public class DataCacheFIFO extends DataCache {
	Queue <DataEntry> index; // LRU queue
	
	public DataCacheFIFO ( String inputFileDir, int allowedSize, boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, "LRU",  outputDir);
		index = new LinkedList<DataEntry> (); 
	}

	public DataEntry evicatOneEntry () throws Exception {
		if( store.size() >= allowedSize ) {
			DataEntry temEntry = index.poll();
			this.deleteFromStore(temEntry);
			this.numOfEvication++;
			return temEntry;
		} else
			return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		index.offer(newEntry);
		this.putintoStore( newEntry);
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
				index.remove(temEntry);
				this.numOfExpiried++;
				temEntry = this.endingTimeQ.peek();
//				System.out.println(this.store.size());
//				System.out.println(this.index.size());
			}
		}
	}
}