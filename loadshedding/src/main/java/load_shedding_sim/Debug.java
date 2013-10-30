package load_shedding_sim;

import java.text.SimpleDateFormat;

public interface Debug {
	public final boolean ORACLE_EPG = true; 
	public final int ORACLE_MAX_ARRAY_INDEX = 15;
	public final int EPG_MAX_SIZE = 115861;
	public final int LOG_MAX_SIZE = 6768470;
	public final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// 12071994
	// for join results
}
