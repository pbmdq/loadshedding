package load_shedding_sim;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import load_shedding_sim.DataCache.timeStampCompare;


public class DataCacheClock extends DataCache{
	String outputFileName = "join_results_clock";
	Queue <DataEntry> index; //qi = new PriorityQueue<Integer>()
	
	public DataCacheClock ( String inputFileDir, int allowedSize , boolean isInner) throws Exception {
		super( inputFileDir, allowedSize, isInner);
		//super();
		index = new LinkedList<DataEntry> (); 
		File file = new File("//Users//Shared//"+outputFileName+".csv");
	    BufferedWriter output = new BufferedWriter(new FileWriter(file));
	    output.close();
	}
	
	public DataEntry getVictimEntry () {
		 //Iterator it= index.iterator();
		DataEntry iteratorEntry = index.peek();
		while(iteratorEntry.numberOfPastResults > 0)
	    {
	    	 iteratorEntry.numberOfPastResults--;
	    	 index.poll();
	    	 index.add(iteratorEntry);
	    	 iteratorEntry = index.peek();
	    }        
		return index.poll();
	}
	
	public void insertOneEntry ( DataEntry newEntry) {
		store.put(newEntry.key, newEntry);
		index.add(newEntry);
		//DataEntry tempEntry = index.peek();
		//System.out.println(newEntry.numberOfTotalResults);
		currentSize++;
	}
	
	public DataEntry evicatOneEntry () {
		if(this.currentSize == this.allowedSize && this.currentSize>0) {
			DataEntry temEntry = getVictimEntry();
			this.store.remove(temEntry.key, temEntry);
			this.currentSize--;
			
			if( this.isInner ) {
				try {
					
					File file = new File("//Users//Shared//"+this.outputFileName+".csv");
				    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
				    output.write(temEntry.timeStamp.toString()+"\t"+temEntry.key+"\t"+temEntry.numberOfLargestPastResults+"\t"+temEntry.numberOfTotalResults+"\t"+"throwaway"+"\n");
				    output.close();
				    
				} catch ( IOException e ) {
					e.printStackTrace();
				}
			}
			
			return temEntry;
		} else
			return null;
	}
	public int performJoin(DataEntry inputEntry) throws Exception {
		Set<DataEntry> entries = store.get(inputEntry.key);
		for(DataEntry entry:entries)
	    {
			entry.numberOfPastResults++;
			entry.numberOfLargestPastResults++;
			//printResutls(entry, inputEntry, outputFileName);
	    }
		return entries.size();
			
	}
}
/*
String dateStr = "2011-09-19T15:57:11Z";
String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
Date date = new SimpleDateFormat(pattern).parse(dateStr);
*/