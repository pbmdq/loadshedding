package load_shedding_sim;
import java.util.*;
import java.io.*;
import java.text.*;

import com.google.common.collect.*;;
// this is test branch
public class DataCache {
	//public int test;
	File fileInput;
	Scanner fileScanner;
	
	File fileOutputStat;
	BufferedWriter bufferOutputStat;
	File fileOutputResults;
	BufferedWriter bufferOutputResults;
	
	public int allowedSize;
	boolean isInner;
	String outputFileNameBase;
	
	SimpleDateFormat sdf;
	
	HashMultimap <String,DataEntry> store;
	
	/*
	public class numberOfTotalResultsCompare implements Comparator<DataEntry> {
	    public int compare(DataEntry x, DataEntry y) {
	        if (x.numberOfTotalResults < y.numberOfTotalResults) {
	            return -1;
	        } else if (x.numberOfTotalResults > y.numberOfTotalResults) {
	            return 1;
	        } else
	        	return 0;
	    }
	}
	
	public class timeStampCompare implements Comparator<DataEntry> {
	    public int compare(DataEntry x, DataEntry y) {
	        if (x.timeStamp.before(y.timeStamp)) {
	            return -1;
	        } else if (x.timeStamp.after(y.timeStamp)) {
	            return 1;
	        } else
	        	return 0;
	    }
	}
	*/

	public void initOutPutFiles() throws Exception {
		fileOutputStat = new File("output//"+outputFileNameBase+".txt");
		bufferOutputStat = new BufferedWriter(new FileWriter(fileOutputStat, true));
		
		fileOutputResults = new File("output//"+outputFileNameBase+"_resutls"+".txt");
		bufferOutputResults = new BufferedWriter(new FileWriter(fileOutputResults));
	}
	public DataCache ( String inputFileDir, int allowedSize, boolean isInner, String outputFileNameBase ) throws Exception {
		fileInput 	 = new File(inputFileDir);
		fileScanner = new Scanner(fileInput);
		this.allowedSize = allowedSize;
		store 		 = HashMultimap.create();
		this.isInner 	 = isInner;
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		this.outputFileNameBase = outputFileNameBase;
		//Comparator<DataEntry> comparator = new timeStampCompare();
		//index = new PriorityQueue<DataEntry> (allowedSize, comparator); 
		initOutPutFiles();
	}
	

	public DataEntry createNewEntry ( String inputStrig ) throws Exception {
		String[] afterSplit = inputStrig.split(",");
		//Date date = new .parse("23/09/2007");
		
		Date timeStamp = sdf.parse(afterSplit[afterSplit.length-3]);
		Date timeStampEnd = sdf.parse(afterSplit[afterSplit.length-2]);
		DataEntry newEntry = new DataEntry(timeStamp, timeStampEnd, afterSplit[afterSplit.length-1], afterSplit[0], 0);
		return newEntry;
	}
	
	public DataEntry evicatOneEntry () throws Exception {
			return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
	}
	public DataEntry next() throws Exception {
		if (fileScanner.hasNext()) {
			String inputStrig = fileScanner.nextLine();
			DataEntry newEntry = createNewEntry (inputStrig);
			return newEntry;
		} else {
			fileScanner.close();
			return null;
		}
	}
	public String convertDateFormate(Date input) {
		return sdf.format(input).toString();
	}

	public void printJoinResutls (DataEntry entry, DataEntry inputEntry ) throws Exception {
		try {
			if(this.isInner) {
				bufferOutputResults.write(convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.key+","+entry.otherDataFields+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.otherDataFields+"\n");
			} else {
				bufferOutputResults.write(convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.key+","+inputEntry.otherDataFields+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.otherDataFields+"\n");	
			}
			//bufferOutputResults.flush();
		} catch ( IOException e ) {
			e.printStackTrace();
		}
	}
	public int performJoin(DataEntry inputEntry, DataCache inputCache) throws Exception {
		Set<DataEntry> entries = store.get(inputEntry.key);
		int numResults = 0;
		//if(entries.size()>0) {
		for(DataEntry entry:entries) {
			if(!(entry.timeStampEnd.before(inputEntry.timeStamp)) && !(entry.timeStamp.after(inputEntry.timeStampEnd))) {
				printJoinResutls(entry, inputEntry);
				
				entry.numberOfPastResults++;
				entry.numberOfLargestPastResults++;
				
				numResults++;
			}
	    }
		inputCache.insertOneEntry(inputEntry);
		inputCache.evicatOneEntry();
		return numResults;
	}
	public void endOfCache() throws Exception {
		bufferOutputStat.close();
		bufferOutputResults.close();
	}
}