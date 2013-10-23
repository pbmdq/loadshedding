package load_shedding_sim;
import java.util.*;

public class DataEntry {
	public Date timeStamp;
	public Date timeStampEnd;
	public String key;
	public String otherDataFields;
	public int numberOfTotalResults;
	public int numberOfPastResults;
	public int numberOfLargestPastResults;
	
	public DataEntry (Date timeStamp, Date timeStampEnd, String key, String otherDataFields, int numberOfTotalResults ) {
		this.timeStamp 			= timeStamp;
		this.timeStampEnd 		= timeStampEnd;
		this.key 				= key;
		this.otherDataFields	= otherDataFields;
		this.numberOfTotalResults = numberOfTotalResults;
		
		numberOfPastResults = 1;
		numberOfLargestPastResults = 0;
	}
}
