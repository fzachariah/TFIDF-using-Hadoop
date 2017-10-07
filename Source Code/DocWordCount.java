//Name: Febin Zachariah
//Email:fzachari@uncc.edu
//Assignment -Fall 2017

package com.cloud.febin;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author Febin Zachariah
 * @since 09/24/2017
 * @description DocWordCount Program calculates the count of each word in each
 *              file and stores the result.
 *
 */

public class DocWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocWordCount.class);
	private static String delimiter = "#####";

	/**
	 * @description Entry point of the Program. It calls the run method by
	 *              passing DocWordCount object and commandline arguments as
	 *              parameters. When the application is finished, it returns an
	 *              integer value for the status, which is passed to the System
	 *              object on exit.
	 *
	 */

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new DocWordCount(), args);
		System.exit(res);

	}

	/**
	 * @description This method configures the job and starts the job and wait
	 *              for the job to complete.
	 *
	 */
	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "docwordcount");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat
				.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @description The Map Class transforms the input data into intermediate
	 *              key/value pairs and is passed into Reducer class for further
	 *              operations.
	 *
	 */
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			LOG.info("Entering Mapper Class");
			String line = lineText.toString();
			String result = "";
			String fileName = ((FileSplit) context.getInputSplit()).getPath()
					.getName();
			for (String word : WORD_BOUNDARY.split(line)) {

				if (word.isEmpty()) {
					continue;
				}

				word = word.toLowerCase();
				result = word + delimiter + fileName;
				context.write(new Text(result), one);
			}
		}
	}

	/**
	 * @description The Reduce Class transforms intermediate key/value pairs
	 *              into required output
	 *
	 */

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			LOG.info("Entering Reducer Class");
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}

}
