package load_shedding_sim;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataEntry {
	public Date timeStamp;
	public Date timeStampEnd;
	public String key;
	public String otherDataFields;
	public int numberOfTotalResults;
	public int numberOfPastResults;
	public int numberOfLargestPastResults;
	public int simTimeStamp;
	public int [][]oracle;
	
	public DataEntry (Date timeStamp, int simTimeStamp, Date timeStampEnd, String key, String otherDataFields, int numberOfTotalResults ) {
		this.timeStamp 			= timeStamp;
		this.simTimeStamp		= simTimeStamp;
		this.timeStampEnd 		= timeStampEnd;
		this.key 				= key;
		this.otherDataFields	= otherDataFields;
		this.numberOfTotalResults = numberOfTotalResults;
		//Random randomGenerator = new Random();
		//numberOfPastResults = randomGenerator.nextInt(3);
		numberOfPastResults 	= 0;
		numberOfLargestPastResults = 0;
	}
	public DataEntry ( String inputString, int simTimeStamp) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[]  afterSplit = inputString.split("\t");
		
		Date timeStamp 			= sdf.parse(afterSplit[1]);
		Date timeStampEnd 		= sdf.parse(afterSplit[2]);
		
		this.simTimeStamp		= Integer.parseInt(afterSplit[0]);
		this.timeStamp 			= timeStamp;
		this.timeStampEnd 		= timeStampEnd;
		this.key 				= afterSplit[3];
		this.otherDataFields	= afterSplit[4];
		this.numberOfTotalResults = 0;
		numberOfPastResults 	= 0;
		numberOfLargestPastResults= 0;
	}
	public DataEntry ( String inputString, int simTimeStamp, int depth) throws Exception {
		//SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[]  afterSplit = inputString.split("\t");
		
		Date timeStamp = Debug.sdf.parse(afterSplit[1]);
		Date timeStampEnd = Debug.sdf.parse(afterSplit[2]);
		
		this.simTimeStamp		= Integer.parseInt(afterSplit[0]);
		this.timeStamp 			= timeStamp;
		this.timeStampEnd 		= timeStampEnd;
		this.key 				= afterSplit[3];
		this.otherDataFields	= afterSplit[4];
		numberOfPastResults 	= 0;
		numberOfLargestPastResults= 0;
		this.numberOfTotalResults= Integer.parseInt(afterSplit[5+depth]);
	}
}
