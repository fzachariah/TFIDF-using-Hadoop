//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment -Fall 2017

package com.cloud.febin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * @description Search Program implements simple batch mode search engine.It accepts
 * user query and returns list of document that matches the query.
 *
 */

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	private static String userInput = "";
	private static String delimiter = "#####";
	
	

	/**
	 * @description Entry point of the Program. It calls the run method by
	 *              passing Search object and commandline arguments as
	 *              parameters. When the application is finished, it returns an
	 *              integer value for the status, which is passed to the System
	 *              object on exit.
	 *
	 */
	public static void main(String[] args) throws Exception {

		int length = args.length;
		if(length<=2)
		{
			System.out.println("Enter a Search Query");
			return;
		}
			
		for (int i = 2; i < length; i++) {
			userInput = userInput + args[i] + " ";
		}
		userInput.trim();
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);

	}

	
	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("UserQuery", String.valueOf(userInput));
		Job job = Job.getInstance(conf, "search");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
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

			Configuration configuration = context.getConfiguration();
			String userQuery = configuration.get("UserQuery").toString();

			String line = lineText.toString();
			String splitterArray[] = line.split(delimiter);
			String word = splitterArray[0];

			for (String value : userQuery.split("\\s+")) {
				if (value.toLowerCase().equals(word)) {

					String infoArray[] = splitterArray[1].split("\\s+");
					String fileName = infoArray[0];
					String tfidf = infoArray[1];
					context.write(new Text(fileName), new Text(tfidf));
				}
			}

		}
	}
	
	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output
	 *
	 */

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			LOG.info("Entering Reduce Class");

			double sum = 0.0;
			for (Text tfidf : values) {
				double tmp = Double.parseDouble(tfidf.toString());
				sum = sum + tmp;
			}
			context.write(word, new Text("" + sum));
		}
	}

}
