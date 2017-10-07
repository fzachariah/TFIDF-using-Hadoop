
//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment -Fall 2017

package com.cloud.febin;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

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
 * @since 09/26/2017
 * @description Rank Program sorts the results coming from Search class in decreasing order.
 *
 */
public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Rank.class);

	
	/**
	 * @description Entry point of the Program. It calls the run method by
	 *              passing Rank object and commandline arguments as
	 *              parameters. When the application is finished, it returns an
	 *              integer value for the status, which is passed to the System
	 *              object on exit.
	 *
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Rank(), args);
		System.exit(res);
	}
	
	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "rank");
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
			context.write(new Text("Item"), lineText);
		}
	}
	
	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output.
	 *
	 */

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context)
				throws IOException, InterruptedException {

			LOG.info("Entering Reduce Class");
			java.util.Map<String, Double> hashMap = new HashMap<String, Double>();
			for (Text value : counts) {
				String infoArray[] = value.toString().split("\\s+");
				String fileName = infoArray[0];
				String tfidf = infoArray[1];
				hashMap.put(fileName, Double.parseDouble(tfidf));
			}

			List<java.util.Map.Entry<String, Double>> list = new LinkedList<java.util.Map.Entry<String, Double>>(
					hashMap.entrySet());

			Collections.sort(list,
					new Comparator<java.util.Map.Entry<String, Double>>() {
						public int compare(
								java.util.Map.Entry<String, Double> o1,
								java.util.Map.Entry<String, Double> o2) {
							return (o2.getValue()).compareTo(o1.getValue());
						}
					});

			java.util.Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
			for (java.util.Map.Entry<String, Double> entry : list) {
				sortedMap.put(entry.getKey(), entry.getValue());
			}

			for (String key : sortedMap.keySet()) {

				context.write(new Text(key), new Text("" + sortedMap.get(key)));
			}
		}
	}
}
