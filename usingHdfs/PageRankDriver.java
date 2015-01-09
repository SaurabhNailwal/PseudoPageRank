
/*
 * @author Saurabh Nailwal
 */

package usingHdfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankDriver extends Configured implements Tool {

	/**
	 * @param args
	 */
	
	
	public int run(String[] arg0) throws Exception {
       
		JobConf job = new JobConf(getConf(), PageRankDriver.class);
		
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		double diff = 10.0, e = 0.1;
		int count = 0;

		String inputPath = arg0[0];
		String outputPath = "Output/PageData";
		
		while (diff > e) {

			if (count == 0) {

				FileInputFormat.setInputPaths(job, new Path(inputPath));
				count++;
			} else {

				FileInputFormat.setInputPaths(job, new Path(outputPath + count));
				count++;
			}
		
			FileOutputFormat.setOutputPath(job, new Path(outputPath + count));
			
			JobClient.runJob(job);			
			
			
			if (count > 2) {
				diff = rankDiff(outputPath, count);
				System.out.println("Diff = "+diff);
			}
			
			if(count >15){
				System.out.println("Max Iterations reached");
				break;
			}

		}

		return 0;
	}

	
	public static double rankDiff(String folder, int count) {

		double diff = 0.0;

		HashMap<String, Double> pageMap1 = new HashMap<String, Double>();
		HashMap<String, Double> pageMap2 = new HashMap<String, Double>();

		// getting the initial values
		pageMap1 = getPageValues(folder + String.valueOf(count - 1)+ "/part-00000");																				
		
		pageMap2 = getPageValues(folder + String.valueOf(count) + "/part-00000");
		
		// calculating the difference between the vectors

		for (String key : pageMap1.keySet()) {

			diff = diff + Math.abs(pageMap2.get(key) - pageMap1.get(key));

		}

		return diff;
	}

	public static HashMap<String, Double> getPageValues(String file) {
		
		HashMap<String, Double> pageMap = new HashMap<String, Double>();

		Path readPath=null;
        FileSystem readFS = null;
        FSDataInputStream fsIn = null;
		BufferedReader bufferedReader = null;
		String line = "";

		try {
            readPath = new Path(file);
            readFS = FileSystem.get(new Configuration());
            fsIn = readFS.open(readPath);
			bufferedReader = new BufferedReader(new InputStreamReader(fsIn));

			while ((line = bufferedReader.readLine()) != null) {

				StringTokenizer strTokenizer = new StringTokenizer(line);

				while (strTokenizer.hasMoreTokens()) {

					pageMap.put(strTokenizer.nextToken(),
							Double.parseDouble(strTokenizer.nextToken()));

					// skipping out-links
					strTokenizer.nextToken();
				}

			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (fsIn != null) {
				try {
					fsIn.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return pageMap;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(), new PageRankDriver(),
				args);

		System.exit(res);

	}

}
