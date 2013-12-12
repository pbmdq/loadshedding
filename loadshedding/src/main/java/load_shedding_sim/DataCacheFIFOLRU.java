package load_shedding_sim;
import java.util.*;

public class DataCacheFIFOLRU extends DataCacheFIFOClock{
	
		public DataCacheFIFOLRU(String inputFileDir, int allowedSize , boolean isInner, String outputDir, double sizeFIFO, int thresHold) throws Exception {
		super(inputFileDir, allowedSize, isInner, outputDir, sizeFIFO, thresHold);
		// TODO Auto-generated constructor stub
	}

		public DataEntry replaceVictimEntryCLOCK ( DataEntry input) {
		DataEntry tempData;
		ListIterator <DataEntry> tempItr = indexCLOCK.listIterator(this.pointerCLOCK);
		for(;;)
		{
			if(!tempItr.hasNext()) 
				tempItr = indexCLOCK.listIterator(0);
			tempData = tempItr.next();
			if(tempData.numberOfPastResults>0) {
				tempData.numberOfPastResults--;
				if(tempData.numberOfPastResults>0)
					tempData.numberOfPastResults = 1;
			}
			else
				break;
		}
		tempItr.set(input);
		this.pointerCLOCK = tempItr.nextIndex();
		return tempData;
	}
}