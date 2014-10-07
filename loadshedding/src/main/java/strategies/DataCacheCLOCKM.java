package strategies;
import java.util.*;

import data_entry.DataEntry;

public class DataCacheCLOCKM extends DataCacheClock{
	// different ways of depreciation
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
			// TODO need dto taken care
			if(tempData.numberOfPastResults>= 1) {
				tempData.numberOfPastResults -=1;
			} else
				break;
		}
		this.statOfTotalEvication++;
		tempItr.set(input);
		this.deleteFromStore(tempData);
		pointer = tempItr.nextIndex();
		index.remove(tempData);
		//System.out.println(this.store.size());
		return tempData;
	}
}