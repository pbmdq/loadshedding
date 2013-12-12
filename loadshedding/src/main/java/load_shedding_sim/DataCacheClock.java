package load_shedding_sim;
import java.util.*;

public class DataCacheClock extends DataCache{
	LinkedList <DataEntry> index;
	int pointer;
	
	public DataCacheClock ( String inputFileDir, int allowedSize , boolean isInner, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, "CLOCK", outputDir);
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
		tempItr.set(input);
		store.remove(tempData.key, tempData);
		pointer = tempItr.nextIndex();
		return tempData;
	}
	
	public DataEntry evicatOneEntry () {
		return null;
	}
	
	public void insertOneEntry ( DataEntry newEntry) {
		if( store.size() == allowedSize ) {
			replaceVictimEntry(newEntry);
		} else
			index.add(newEntry);
		store.put(newEntry.key, newEntry);
	}
}