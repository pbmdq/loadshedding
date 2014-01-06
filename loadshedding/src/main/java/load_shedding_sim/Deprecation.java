package load_shedding_sim;

public class Deprecation {
	int [] histogram; 
	int numOfBinLimit = 200;
	int numOfBin;
	int binSize;
	int lastCalculationTime;
	
	public Deprecation(int totalCacheSize){
		totalCacheSize = totalCacheSize*3/2;
		if(numOfBinLimit < totalCacheSize) {
			binSize 	= totalCacheSize/numOfBinLimit;
			numOfBin	= numOfBinLimit;
		}else {
			numOfBin	= numOfBinLimit;
			binSize		= 1;
		}
		histogram 	= new int[numOfBin];
		
	}
	public void addOneStat(int timeInterval){
		if(timeInterval/binSize< numOfBin)
			histogram[timeInterval/binSize]++;
		//if(timeInterval > 120)
		//	System.out.print("here");
		//else
		//	histogram[numOfBin-1]++;
	}
	public double calculateDepreciate(int currentGlobalTimeStamp, int calculateInterval){
		/*
		if ((currentGlobalTimeStamp - lastCalculationTime) >= calculateInterval)
		{
			lastCalculationTime = currentGlobalTimeStamp;
			for(int i = 0; i< this.numOfBin; i++) {
				System.out.print(histogram[i]+"\t");
			}
			System.out.print("\n");
			return 1;
		} else
			return 0;
			*/
		return 0;
	}

}
