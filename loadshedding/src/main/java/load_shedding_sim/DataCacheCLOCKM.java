package load_shedding_sim;
import java.util.*;

public class DataCacheCLOCKM extends DataCacheClock{
	
	public DataCacheCLOCKM ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, outputDir);
	}
	public DataEntry replaceVictimEntry ( DataEntry input) {
		DataEntry tempData;
		ListIterator <DataEntry> tempItr = index.listIterator(pointer);
		for(;;)
		{
			if(!tempItr.hasNext()) 
				tempItr = index.listIterator(0);
			tempData = tempItr.next();
			if(tempData.numberOfPastResults>= 1) {
				tempData.numberOfPastResults -=1;
			} else
				break;
		}
		this.numOfEvication++;
		tempItr.set(input);
		this.deleteFromStore(tempData);
		pointer = tempItr.nextIndex();
		return tempData;
	}
}