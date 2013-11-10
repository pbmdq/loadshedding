package load_shedding_sim;
import java.text.SimpleDateFormat;
import java.util.*;

public class DataEntryBase {
	public Date timeStamp;
	public Date timeStampEnd;
	public int numberOfTotalResults;
	public int numberOfPastResults;
	public int numberOfLargestPastResults;
	public int simTimeStamp;
	public int [][]oracle;
	public String key;
	public String otherDataFields;
	
}
