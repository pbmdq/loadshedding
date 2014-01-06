package load_shedding_sim;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataEntryVistaTV extends DataEntry{
	public DataEntryVistaTV ( String inputString, int simTimeStamp) throws Exception {
		String[]  afterSplit = inputString.contains("\t")?inputString.split("\t"):inputString.split(",");
		//this.simTimeStamp		= simTimeStamp;
		this.localSimTimeStamp  = simTimeStamp;
		this.timeStamp 			= Debug.sdf.parse(afterSplit[1]);
		this.timeStampEnd 		= Debug.sdf.parse(afterSplit[2]);
		this.key 				= afterSplit[3];
		this.otherDataFields	= afterSplit[4];
		//this.localSimTimeStamp	= Integer.parseInt(afterSplit[5]);
		this.numberOfTotalResults = 0;
		numberOfPastResults 	= 1;
		numberOfLargestPastResults= 1;
	}
	public DataEntryVistaTV ( String inputString, int simTimeStamp, int depth) throws Exception {
		this(inputString, simTimeStamp);
		String[]  afterSplit = inputString.contains("\t")?inputString.split("\t"):inputString.split(",");
		this.numberOfTotalResults= Integer.parseInt(afterSplit[6+depth]);
	}
}
