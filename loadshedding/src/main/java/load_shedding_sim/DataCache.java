package load_shedding_sim;
import java.util.*;
import java.io.*;
import java.text.*;

import com.google.common.base.Charsets;
import com.google.common.collect.*;
import com.google.common.io.*;

public class DataCache {
	//File fileInput;
	BufferedReader fileBufferReader;
	
	File fileOutputStat;
	//BufferedWriter bufferOutputStat;
	File fileOutputResults;
	//BufferedWriter bufferOutputResults;
	
	public int allowedSize;
	boolean isInner;
	String outputFileNameBase;
	String outputDir;
	
	HashMultimap <String,DataEntry> store;
	
	public void initOutPutFiles() throws Exception {
		java.util.Date date= new java.util.Date();
		fileOutputStat = new File(outputDir+outputFileNameBase+".txt");
		fileOutputResults = new File(outputDir+outputFileNameBase+"_resutls"+".txt");
	}
	public DataCache ( String inputFileDir, int allowedSize, boolean isInner, String outputFileNameBase, String outputDir ) throws Exception {
		//fileInput 	 = new File(inputFileDir);
		fileBufferReader = new BufferedReader( new FileReader(inputFileDir));
		this.allowedSize = allowedSize;
		store 		 = HashMultimap.create();
		this.isInner = isInner;
		this.outputDir= outputDir;
		
		this.outputFileNameBase = outputFileNameBase;
		initOutPutFiles();
	}

	public DataEntry evicatOneEntry () throws Exception {
			return null;
	}
	
	public void insertOneEntry ( DataEntry newEntry) {
	}
	
	public DataEntry next(int simTimeStamp) throws Exception {
		String inputStrig;
		if ((inputStrig = fileBufferReader.readLine()) != null) {
			DataEntry newEntry = new DataEntry (inputStrig, simTimeStamp);
			return newEntry;
		} else {
			fileBufferReader.close();
			return null;
		}
	}
	public String convertDateFormate(Date input) {
		return Debug.sdf.format(input).toString();
	}

	public void printJoinResutls (DataEntry entry, DataEntry inputEntry ) throws Exception {
		try {
			
			if(this.isInner) {
				Files.append(inputEntry.simTimeStamp+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.key+","+entry.otherDataFields+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.otherDataFields+"\n", fileOutputResults, Charsets.UTF_8);
			} else {
				Files.append(inputEntry.simTimeStamp+","+convertDateFormate(inputEntry.timeStamp)+","+convertDateFormate(inputEntry.timeStampEnd)+","+inputEntry.key+","+inputEntry.otherDataFields+","+convertDateFormate(entry.timeStamp)+","+convertDateFormate(entry.timeStampEnd)+","+entry.otherDataFields+"\n", fileOutputResults, Charsets.UTF_8);
			}
			
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
				//printJoinResutls(entry, inputEntry);
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
		//bufferOutputResults.flush();
		//bufferOutputResults.close();
		
	}
}