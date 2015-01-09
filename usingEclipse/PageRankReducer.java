
/*
 * @author Saurabh Nailwal
 */

package usingEclipse;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	private Text pageRank = new Text();
	private Text outlinks = new Text();
	
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
		double sumRanks=0.0,rank_i = 0.0;
		float beta = 0.8F;
		String[] pairs = null, pairSplit= null;
								
		//calculating page rank through in-links
		while(values.hasNext()){
			
			pairs = values.next().toString().split(" ");
			
			for(int i=0;i<pairs.length;i++){
				
				if(pairs[i].charAt(0)== ':'){
					
					outlinks.set(pairs[i].substring(1));
					continue;
				}
				
				pairSplit = pairs[i].split(",");
				
				rank_i += Double.parseDouble(pairSplit[1])*beta;
			}			
			
		}
		
		sumRanks +=  rank_i;		
		
		PageRankDriver.noOfPages++;
		PageRankDriver.sumOfRanks = sumRanks;	
	
		pageRank.set(key+" "+rank_i);
		
		
		//leaked pageRanks
		output.collect(pageRank, outlinks);
	}

}
