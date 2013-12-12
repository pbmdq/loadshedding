package load_shedding_sim;
import java.util.*;

public class DataCacheLRU extends DataCacheClock{
	
	public DataCacheLRU ( String inputFileDir, int allowedSize , boolean isInner, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, outputDir);
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
				if(tempData.numberOfPastResults>0)
					tempData.numberOfPastResults = 1;
			} else
				break;
		}
		tempItr.set(input);
		store.remove(tempData.key, tempData);
		pointer = tempItr.nextIndex();
		return tempData;
	}
}