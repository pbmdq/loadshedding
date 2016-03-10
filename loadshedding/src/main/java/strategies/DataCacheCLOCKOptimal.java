package strategies;

import java.util.*;

import data_entry.DataEntry;

public class DataCacheCLOCKOptimal extends DataCacheClock {

	int totalMiss = 0;
	public DataCacheCLOCKOptimal(String inputFileDir, int allowedSize,
			boolean isInner, boolean enableReasoning, String outputDir)
			throws Exception {
		super(inputFileDir, allowedSize, isInner, enableReasoning, outputDir);
	}

	public void warmupReset () {
		statOfTotalEvication 	= 0;
		statOfTotalExpiried		= 0;
		totalMiss				= 0;
	}
	public void endOfCache() throws Exception {
		System.out.println("total miss: " + this.totalMiss);
	}
	public DataEntry replaceVictimEntry(DataEntry input) {
		DataEntry tempData;
		DataEntry victimData = null;
		ListIterator<DataEntry> tempItr = index.listIterator();
		int leastFutureResults = Integer.MAX_VALUE;

		while (tempItr.hasNext()) {
			tempData = tempItr.next();
			if (tempData.numberOfFutureResults < leastFutureResults) {
				leastFutureResults = tempData.numberOfFutureResults;
				victimData = tempData;
			}
		}
		assert(victimData.numberOfFutureResults >=0 );
		if( victimData.numberOfFutureResults != 0 )
			totalMiss+= victimData.numberOfFutureResults;
		// System.out.println(index.size());
		this.statOfTotalEvication++;
		this.deleteFromStore(victimData);
		index.remove(victimData);
		return victimData;
	}
}