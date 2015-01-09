
/*
 * @author Saurabh Nailwal
 */

package usingEclipse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
	public static double sumOfRanks = 0.0;
	public static int noOfPages = 0;
	
	public int run(String[] arg0) throws Exception {

		JobConf job = new JobConf(getConf(), PageRankDriver.class);
		job.setJobName("PageRankJob");

		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		double diff = 10.0, e = 0.1;
		int count = 0;

		String inputPath = "/home/saurabh/PageRank/InputData";// new
																// Path(arg0[0])
		String outputPath = "/home/saurabh/PageRank/PageData";// new
																// Path(arg0[1])

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

			leakedPageRank(outputPath+count);
			
			if (count > 2) {
				diff = rankDiff(outputPath, count);
			}
			
			if(count >15){
				System.out.println("breaking");
				break;
			}

		}

		return 0;
	}
	
	public static void leakedPageRank(String file){
		
		String fileToRead = file + "/part-00000";
		String fileToWrite = file + "/NewOutput";
		double S = sumOfRanks;
		int N = noOfPages;
		
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		String line = "";
		
		FileWriter fileWriter = null;
		BufferedWriter bufferedWriter = null;
		StringBuilder sB = new StringBuilder(); 
		double rank = 0.0, newRank = 0.0;
		
		try {

			fileReader = new FileReader(fileToRead);
			bufferedReader = new BufferedReader(fileReader);
			fileWriter = new FileWriter(fileToWrite);
			bufferedWriter = new BufferedWriter(fileWriter);

			while ((line = bufferedReader.readLine()) != null) {

				StringTokenizer strTokenizer = new StringTokenizer(line);

				while (strTokenizer.hasMoreTokens()) {

					sB.append(strTokenizer.nextToken()+" ");
					rank=Double.parseDouble(strTokenizer.nextToken());

					newRank = rank + (1-S)/N;
					
					sB.append(newRank+" ");
					sB.append(strTokenizer.nextToken()+"\n");
																								
				}		
				

			}
			
			System.out.println("string Builder:\n"+ sB.toString());
			//writing into new file
			bufferedWriter.write(sB.toString());
			bufferedWriter.flush();
			
			File fileToDel = new File(fileToRead);
			
			fileToDel.delete();
			
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
			if (fileReader != null) {
				try {
					fileReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (bufferedWriter != null) {
				try {
					bufferedWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (fileWriter != null) {
				try {
					fileWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
		
		
	}

	public static double rankDiff(String folder, int count) {

		double diff = 0.0;

		HashMap<String, Double> pageMap1 = new HashMap<String, Double>();
		HashMap<String, Double> pageMap2 = new HashMap<String, Double>();

		// getting the initial values
		pageMap1 = getPageValues(folder + String.valueOf(count - 1)+ "/NewOutput");																				

		pageMap2 = getPageValues(folder + String.valueOf(count) + "/NewOutput");

		// calculating the difference between the vectors
		for (String key : pageMap1.keySet()) {

			diff = diff + Math.abs(pageMap2.get(key) - pageMap1.get(key));

		}

		return diff;
	}

	
	public static HashMap<String, Double> getPageValues(String file) {
		
		HashMap<String, Double> pageMap = new HashMap<String, Double>();

		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		String line = "";

		try {

			fileReader = new FileReader(file);
			bufferedReader = new BufferedReader(fileReader);

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
			if (fileReader != null) {
				try {
					fileReader.close();
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
