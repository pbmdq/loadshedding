package data_entry;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataEntry {
	public Date timeStamp;
	public Date timeStampEnd;
	//public Date realTimeStamp; entering time stamp maybe
	public int numberOfTotalResults;
	public double numberOfPastResults;
	public int numberOfLargestPastResults;
	public int globalSimTimeStamp;
	public int localSimTimeStamp;
	public int [][]oracle;
	public int uniqueID;
	public String key;
	public String otherDataFields;
	// FIFO+LRU/CLOCK
	public boolean isInFIFO = true;
	// FIFO+FIFOs
	public int segID = 0;
	// for ARC
	public int ARCQ = 0;
	public int ARCReference = 0;
	
	public void afterJoin( int numberOfResults){
		this.numberOfLargestPastResults	+= numberOfResults;
		this.numberOfPastResults 		+= numberOfResults;
	}
	public void beforeSwitchToCLOCK( ){
		this.numberOfLargestPastResults	= 1;
		this.numberOfPastResults 		= 1;
		this.isInFIFO 					= false;
	}
	public void beforeSwitchNextSeg( int segID ){
		this.numberOfLargestPastResults	= 1;
		this.numberOfPastResults 		= 1;
		this.segID = segID;
	}
}
