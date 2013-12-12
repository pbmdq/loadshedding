package load_shedding_sim;

import java.text.SimpleDateFormat;
import java.util.Date;

public interface Debug {
	public final int ORACLE_EPG 			= 1;
	public final int ORACLE_LOG 			= 2;
	public final int ORACLE_SR_WINDSPEED 	= 3;
	
	//public final Long ORACLE_SR_WINDSPEED_DEBUG = 1091923700L;
	
	public final int ORACLE_MAX_ARRAY_INDEX = 40;
	
	public final int JOIN_TYPE_ONE_WAY = 1;
	public final int JOIN_TYPE_TWO_WAY = 21;
	
	public final int EPG_MAX_SIZE 			= 115861;
	//public final int EPG_MAX_SIZE 			= 115861;
	//6122 of epg is in one day join
	//581771 of log is in one day join
	public final int LOG_MAX_SIZE 			= 6768470;
	public final int WINDSPEED_MAX_SIZE 	= 1741461;
	public final SimpleDateFormat sdf 		= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// 12071994
	// for join results
	
	public final int FF_NUM_SEG = 3;
}
