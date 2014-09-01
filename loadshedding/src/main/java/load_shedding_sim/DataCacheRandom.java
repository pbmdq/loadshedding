package load_shedding_sim;
import java.util.*;

public class DataCacheRandom extends DataCacheClock {
	public DataCacheRandom ( String inputFileDir, int allowedSize , boolean isInner, boolean enableReasoning, String outputDir) throws Exception {
		super( inputFileDir, allowedSize, isInner, enableReasoning, outputDir);
	}
	
	public DataEntry replaceVictimEntry ( DataEntry input) {
		DataEntry tempData = null;
		ListIterator <DataEntry> tempItr = index.listIterator(0);
		int evicatIndex = (int)(Math.random()*(index.size()));
		//evicatIndex = 56;
		//System.out.println(evicatIndex);
		for(int i = 0; i < evicatIndex-1; i++) {
			tempData = tempItr.next();
		}
		
		if(tempData != null) {
			this.statOfTotalEvication++;
			tempItr.set(input);
			this.deleteFromStore(tempData);
			//store.remove(tempData.key, tempData);
			return tempData;
		}else
			return null;
	}
}