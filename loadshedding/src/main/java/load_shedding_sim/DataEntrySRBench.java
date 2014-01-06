package load_shedding_sim;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang3.time.DateUtils;

public class DataEntrySRBench extends DataEntry{
	public DataEntrySRBench ( String inputString, int simTimeStamp, int depth) throws Exception {
		this ( inputString, simTimeStamp);
		String[]  afterSplit = inputString.split("\t");
		this.numberOfTotalResults= Integer.parseInt(afterSplit[4+depth]);
	}
	public DataEntrySRBench ( String inputString, int simTimeStamp) throws Exception {
		String[]  afterSplit = inputString.split(",");
		//System.out.println(afterSplit[1]);
		//this.simTimeStamp		= simTimeStamp;
		this.localSimTimeStamp	= simTimeStamp;
		this.timeStamp 			= new Date(Long.parseLong(afterSplit[1]));
		//System.out.println(this.timeStamp.getTime());
		this.timeStampEnd 		= DateUtils.addMilliseconds(this.timeStamp, 300);
		this.key 				= afterSplit[3];
		this.otherDataFields	= afterSplit[2];
	}
	
}
