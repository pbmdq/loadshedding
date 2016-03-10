package strategy;
import java.util.*;

import data_entry.DataEntry;

public class DataCacheCLOCKONE extends DataCacheClock{
	
	public DataCacheCLOCKONE ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
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
			if(tempData.numberOfPastResults>0) {
				tempData.numberOfPastResults--;
				if(tempData.numberOfPastResults>0)
					tempData.numberOfPastResults = 1;
			} else
				break;
		}
		this.statOfTotalEvication++;
		tempItr.set(input);
		this.deleteFromStore(tempData);
		pointer = tempItr.nextIndex();
		return tempData;
	}
}