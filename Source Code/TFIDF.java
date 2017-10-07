
//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment -Fall 2017

package com.cloud.febin;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author Febin Zachariah
 * @since 09/25/2017
 * @description TFIDF Program calculates the  word/file pair with its corresponding TFIDF values.
 *
 */
public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TermFrequency.class);
	private static String delimiter = "#####";
	private static String tempPath="/user/cloudera/Assignment/output/temp";

	
	/**
	 * @description Entry point of the Program. First It calls the run method by
	 *              passing TermFrequency object and commandline arguments as
	 *              parameters followed by calling the run method on TFID object(Chaining). When the application is finished, it returns an
	 *              integer value for the status, which is passed to the System
	 *              object on exit.
	 *
	 */
	public static void main(String[] args) throws Exception {

		String[] tempargs=new String[2];
		tempargs[0]=args[0];
		tempargs[1]=tempPath;
		int tf = ToolRunner.run(new TermFrequency(), tempargs);
		if (tf == 0) {
			int res = ToolRunner.run(new TFIDF(), args);
			System.exit(res);
		}

	}
	
	
	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */

	@Override
	public int run(String[] args) throws Exception {

		FileSystem fs = FileSystem.get(getConf());
		Path pt = new Path(args[0]);
		ContentSummary cs = fs.getContentSummary(pt);
		long fileCount = cs.getFileCount();
		Configuration conf = new Configuration();
		conf.set("Count", String.valueOf(fileCount));
		Job job = Job.getInstance(conf, "tfidf");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(tempPath));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
	/**
	 * @description The Map Class transforms the input data into intermediate
	 *              key/value pairs and is passed into Reducer class for further
	 *              operations.
	 *
	 */

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			LOG.info("Entering Map Class");
			String line = lineText.toString();
			String splitterArray[] = line.split(delimiter);
			String word = splitterArray[0];
			String data = splitterArray[1];
			String temp[] = data.split("\t");
			String fileName = temp[0];
			String tfVal = temp[1];
			String value = fileName + "=" + tfVal;
			context.write(new Text(word), new Text(value));

		}
	}
	
	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output.TFIDF value is calculated here.
	 *
	 */

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			LOG.info("Entering Reducer Class");
			Configuration configuration = context.getConfiguration();
			long totalDocuments = Long.parseLong(configuration.get("Count")
					.toString());
			HashMap<String, Double> hashMap = new HashMap<String, Double>();
			double idfResult = 0.0;
			long fileCount = 0;
			for (Text value : values) {
				String splitterArray[] = value.toString().split("=");
				String fileName = splitterArray[0];
				double tfResult = Double.parseDouble(splitterArray[1]);
				String finalKey = word.toString() + delimiter + fileName;
				hashMap.put(finalKey, tfResult);
				fileCount++;
			}
			idfResult = Math.log10(1 + (totalDocuments / fileCount));
			for (String key : hashMap.keySet()) {
				double val = hashMap.get(key) * idfResult;
				context.write(new Text(key), new Text("" + val));
			}

		}
	}

}
