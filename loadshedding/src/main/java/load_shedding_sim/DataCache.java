package load_shedding_sim;
import java.util.*;
import java.io.*;
import java.text.*;

import com.google.common.collect.*;;

public class DataCache {
	public int allowedSize;
	public int currentSize;
	File fileInput;
	Scanner fileScanner;
	boolean isInner;
	
	String outputFileName = "join_results_other";
	String outputJoinResultsFileName = "join_results";
	
	HashMultimap <String,DataEntry> store;
	PriorityQueue <DataEntry> index; //qi = new PriorityQueue<Integer>()
	
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
	
	
	public DataCache (  ) {
	}
	
	public DataCache ( String inputFileDir, int allowedSize, boolean isInner ) throws Exception {
		fileInput 	 = new File(inputFileDir);
		fileScanner = new Scanner(fileInput);
		currentSize = 0;
		this.allowedSize = allowedSize;
		store 		 = HashMultimap.create();
		this.isInner 	 = isInner;
		Comparator<DataEntry> comparator = new timeStampCompare();
		index = new PriorityQueue<DataEntry> (allowedSize, comparator); 
		File file = new File("//Users//Shared//"+outputFileName+".csv");
	    BufferedWriter output = new BufferedWriter(new FileWriter(file));
	    output.close();
	}
	

	public DataEntry createNewEntry ( String inputStrig ) throws Exception {
		String[] afterSplit = inputStrig.split(",");
		//Date date = new .parse("23/09/2007");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date timeStamp = sdf.parse(afterSplit[afterSplit.length-3]);
		Date timeStampEnd = sdf.parse(afterSplit[afterSplit.length-2]);
		DataEntry newEntry = new DataEntry(timeStamp, timeStampEnd, afterSplit[afterSplit.length-1], afterSplit[0], 0);
		return newEntry;
	}
	
	public DataEntry evicatOneEntry () throws Exception {
		if( currentSize == allowedSize &&  currentSize>0) {
			// evicat
			DataEntry temEntry = index.poll();
			store.remove(temEntry.key, temEntry);
			currentSize--;
			
			/*
			if( isInner ) {
				try {
					
					File file = new File("//Users//Shared//"+ outputFileName+"_evication"+".csv");
				    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
				    output.write(temEntry.timeStamp.toString()+"\t"+temEntry.key+"\t"+temEntry.numberOfPastResults+"\t"+temEntry.numberOfTotalResults+"\t"+"throwaway"+"\n");
				    output.close();
				    
				} catch ( IOException e ) {
					e.printStackTrace();
				}
			}
			*/
			return temEntry;
		} else
			return null;
	}
	public void insertOneEntry ( DataEntry newEntry) {
		store.put(newEntry.key, newEntry);
		index.add(newEntry);
		currentSize++;
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
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(input).toString();
	}

	public void printJoinResutls (DataEntry entry, DataEntry inputEntry ) throws Exception {
		try {
			File file = new File("//Users//Shared//"+outputJoinResultsFileName+".csv");
		    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			//Date timeStamp = sdf.parse(afterSplit[afterSplit.length-3]);
		    //output.write(convertDateFormate(entry.timeStamp)+"\t"+convertDateFormate(entry.timeStampEnd)+"\t"+entry.key+"\t"+entry.otherDataFields+"\t"+convertDateFormate(inputEntry.timeStamp)+"\t"+convertDateFormate(inputEntry.timeStampEnd)+"\t"+inputEntry.otherDataFields+"\n\n");
		    //if()
			if(this.isInner) {
				output.write(convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.key+","+entry.otherDataFields+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.otherDataFields+"\n");
			} else {
				output.write(convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.key+","+inputEntry.otherDataFields+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.otherDataFields+"\n");	
			}
			output.close();
		    
		} catch ( IOException e ) {
			e.printStackTrace();
		}
		
		//System.out.println(convertDateFormate(entry.timeStamp)+"\t"+convertDateFormate(entry.timeStampEnd)+"\t"+entry.key+"\t"+entry.otherDataFields+"\n"+convertDateFormate(inputEntry.timeStamp)+"\t"+convertDateFormate(inputEntry.timeStampEnd)+"\t"+inputEntry.otherDataFields+"\n\n");
	    
	}
	public int performJoin(DataEntry inputEntry, DataCache inputCache) throws Exception {
		Set<DataEntry> entries = store.get(inputEntry.key);
		int numResults = 0;
		if(entries.size()>0) {
			for(DataEntry entry:entries) {
				if(!(entry.timeStampEnd.before(inputEntry.timeStamp)) && !(entry.timeStamp.after(inputEntry.timeStampEnd))) {
					entry.numberOfPastResults++;
					entry.numberOfLargestPastResults++;
					printJoinResutls(entry, inputEntry);
					numResults++;
				}
				//afterSplit.length(entry, inputEntry, outputFileName);
		    }
		}
		inputCache.insertOneEntry(inputEntry);
		inputCache.evicatOneEntry ();
		return numResults;
			
	}
	
	
}
/*
String dateStr = "2011-09-19T15:57:11Z";
String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
Date date = new SimpleDateFormat(pattern).parse(dateStr);
*/