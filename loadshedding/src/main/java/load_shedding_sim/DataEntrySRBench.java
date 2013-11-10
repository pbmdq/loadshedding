package load_shedding_sim;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataEntrySRBench extends DataEntryBase{
	public DataEntrySRBench ( String inputString, int simTimeStamp, int depth) throws Exception {
		this ( inputString, simTimeStamp);
		String[]  afterSplit = inputString.split("\t");
		this.numberOfTotalResults= Integer.parseInt(afterSplit[2+depth]);
	}
	public DataEntrySRBench ( String inputString, int simTimeStamp) throws Exception {
		String[]  afterSplit = inputString.split(",");
		this.simTimeStamp		= Integer.parseInt(afterSplit[0]);
		this.timeStamp 			= new Date(Long.parseLong(afterSplit[1]));
		this.timeStampEnd 		= this.timeStamp;
		this.key 				= afterSplit[3];
		this.otherDataFields	= afterSplit[2];
	}
	
}
