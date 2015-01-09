
/*
 * @author Saurabh Nailwal
 */

package usingEclipse;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private Text page_i = new Text();
	private Text value_i = new Text();

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		// Get in-links for pages and rank for each link

		String page_o = "";
		String outlinks = "";
		String[] outlinksSplit = null;
		double rank = 0;
		
		//Initializing
		PageRankDriver.noOfPages = 0;
		PageRankDriver.sumOfRanks = 0.0;
		
		StringTokenizer strTokenizer = new StringTokenizer(value.toString());

		while (strTokenizer.hasMoreTokens()) {

			page_o = strTokenizer.nextToken();
			rank = Double.parseDouble(strTokenizer.nextToken());

			//System.out.println("rank ="+ rank);
			// skipping out-links
			if(strTokenizer.hasMoreTokens()){
				outlinks =strTokenizer.nextToken();
				outlinksSplit = outlinks.split(",");
			}
			
					
		}
		

		double degree = outlinksSplit.length;
		double linkRank = rank/degree;
			
		for(int i=0; i<outlinksSplit.length;i++){
			
			page_i.set(outlinksSplit[i]);
			value_i.set(page_o+","+linkRank);output.collect(page_i , value_i);
		}
		
		//keeping the outlinks
		page_i.set(page_o);
		value_i.set(":"+ outlinks);output.collect(page_i , value_i);
	}

}
