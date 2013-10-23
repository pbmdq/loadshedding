package load_shedding_sim;

import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;
import org.apache.commons.lang3.time.DateUtils;


// 115861 inputs
// for epg.csv
// 6768470 inputs
// for log.csv
// 12071994

public class LoadSheddingSimMain {
	public DataCache innerCache;
	public DataCache outerCache;
	public int outputCounter;
	public int executionLenghth;
	public Date endingTime;
	
		
	public LoadSheddingSimMain (String innerTableDir, int innerCacheSize, String outerTableDir, int outterCacheSize, int executionLength) throws Exception {
		this.innerCache = new DataCache (innerTableDir, innerCacheSize, true);
		this.outerCache = new DataCache (outerTableDir, outterCacheSize, false);
		this.executionLenghth= executionLength;
	}
	public void performJoin () {
	}
	
	public static void main (String[] args) throws Exception{
		LoadSheddingSimMain myLoadSheddingSim= new LoadSheddingSimMain ("//Users//Shared//input//epg.csv", 200000 , "//Users//Shared//input//log.csv", 300000, 1);
		
		DataEntry innerEntry = myLoadSheddingSim.innerCache.next();
		DataEntry outterEntry = myLoadSheddingSim.outerCache.next();
		//myLoadSheddingSim.endingTime = innerEntry.timeStamp;
		myLoadSheddingSim.endingTime = DateUtils.addDays(innerEntry.timeStamp, myLoadSheddingSim.executionLenghth);
		System.out.println(myLoadSheddingSim.endingTime.toString());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//System.out.println(sdf.format(myLoadSheddingSim.endingTime).toString());
		//Date startingStamp = innerEntry.timeStamp;
		//int counterOfresults = 0;
		//int counterOfInnerInput = 0;
		while ( innerEntry != null && outterEntry != null && (innerEntry.timeStamp.before(myLoadSheddingSim.endingTime) || outterEntry.timeStamp.before(myLoadSheddingSim.endingTime))) {
			//System.out.println(sdf.format(outterEntry.timeStamp).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields);
			//System.out.println(sdf.format(innerEntry.timeStamp).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields);
			/*
			 * count time stamp
			if( (innerEntry.timeStamp.getTime() - startingStamp.getTime() ) > 1000*60*30 ) {
				System.out.println(counterOfInnerInput);
				startingStamp 		= innerEntry.timeStamp;
				counterOfresults 	= 0;
				counterOfInnerInput = 0;
			}
			*/
			/*
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				innerEntry = myLoadSheddingSim.innerCache.next();
			} else {
			outterEntry = myLoadSheddingSim.outerCache.next();
			}
			*/
			//if(innerEntry.key.equals("sf-1") && outterEntry.otherDataFields.equals("DNLWs6jF3sjVt2IWXc/C3g=="))
			//c	System.out.println("here");
			
			if(innerEntry.timeStamp.before(outterEntry.timeStamp)) {
				//System.out.println(sdf.format(outterEntry.timeStamp).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields);
				//System.out.println(sdf.format(innerEntry.timeStamp).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields);
				
				if( myLoadSheddingSim.outerCache.currentSize>0 ) {
					int numOfJoinResults = myLoadSheddingSim.outerCache.performJoin(innerEntry, myLoadSheddingSim.innerCache);
					if(numOfJoinResults>0) {
						//System.out.println(numOfJoinResults);
						myLoadSheddingSim.outputCounter +=numOfJoinResults;
						//counterOfresults+=numOfJoinResults;
					}
				}
				//outterEntry = myLoadSheddingSim.outerCache.next();
				
				innerEntry = myLoadSheddingSim.innerCache.next();
				//System.out.println(sdf.format(innerEntry.timeStamp).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields);
				
				//counterOfInnerInput++;
			} else {
				//System.out.println(sdf.format(outterEntry.timeStamp).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields);
				//System.out.println(sdf.format(innerEntry.timeStamp).toString()+"\t"+innerEntry.key+"\t"+innerEntry.otherDataFields);
				
				int numOfJoinResults = myLoadSheddingSim.innerCache.performJoin(outterEntry, myLoadSheddingSim.outerCache);
				if(numOfJoinResults>0) {
					//System.out.println(numOfJoinResults);
					myLoadSheddingSim.outputCounter +=numOfJoinResults;
					//counterOfresults+=numOfJoinResults;
				}
				outterEntry = myLoadSheddingSim.outerCache.next();
				//System.out.println(sdf.format(outterEntry.timeStamp).toString()+"\t"+outterEntry.key+"\t"+outterEntry.otherDataFields);
			}
			
		}
		System.out.println(innerEntry.timeStamp.toString());
		System.out.println(outterEntry.timeStamp.toString());
		
		File file = new File("//Users//Shared//loadshedding_pre_results.csv");
	    BufferedWriter output = new BufferedWriter(new FileWriter(file, true));
	    //output.write(entry.timeStamp.toString()+"\t"+inputEntry.timeStamp.toString()+"\t"+entry.key+"\n");
	    output.write(myLoadSheddingSim.outputCounter+"\n");
	    
	    output.close();
	    
		System.out.println("End");
		System.out.println(myLoadSheddingSim.outputCounter);
		
		
	}
}
// number of ab input 90808
// number of bc input 177269

