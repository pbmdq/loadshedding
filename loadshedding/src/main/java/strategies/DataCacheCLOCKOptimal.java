package strategies;

import java.util.*;

import data_entry.DataEntry;

public class DataCacheCLOCKOptimal extends DataCacheClock {

	public DataCacheCLOCKOptimal(String inputFileDir, int allowedSize,
			boolean isInner, boolean enableReasoning, String outputDir)
			throws Exception {
		super(inputFileDir, allowedSize, isInner, enableReasoning, outputDir);
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

		// System.out.println(index.size());
		this.statOfTotalEvication++;
		this.deleteFromStore(victimData);
		index.remove(victimData);
		return victimData;
	}
}