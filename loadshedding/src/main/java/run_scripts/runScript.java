package run_scripts;


public class runScript {
	public static void main (String[] args) throws Exception{
	    int totalNumOfRuns 	= 7;
	    String baseArgument = "java -jar -Xmx1024m loadshedding/target/loadshedding-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
	    int valueT 			= 1;
	    String inputOutputDir = "-I //Users//shengao//git//loadshedding//loadshedding//input//wind_speed_simTime.csv -O //Users//shengao//git//loadshedding//loadshedding//input//log_with_oracle_3days.csv -OUT //Users//shengao//git//loadshedding//loadshedding//output//";
	    double innerSize 	= 0.001;
	    double outterSize 	= 0.1;
	    int valueL 			= 3;
	    String strategy 	= "RANDOM";
	    String twoPartsStuff= "-SF 0.9 -W 600000 -TH 0";
	    boolean withR 		= true;
	    
	    for (int j = 0; j< totalNumOfRuns; j++) {
	    	
	    	String outputString = baseArgument +" -T "+valueT+" "+ inputOutputDir+" -Si "+ innerSize+" -So " + outterSize + " -L "+ valueL + " "+strategy+" "+twoPartsStuff;
	    	System.out.println(outputString);
	    	
	    }
	    
	}
}
