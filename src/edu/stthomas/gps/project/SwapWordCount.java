package edu.stthomas.gps.project;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * @author Robert Driesch - UST Id# 101058113
 * @version 1.0, December 1, 2014
 **/
public class SwapWordCount extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(SwapWordCount.class);

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class SwapCountMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

		/**
		 * Read the <key, value> pair that represents the word counts from the incident description words and swap the
		 * key and the value so the counts will be sorted.
		 * <p>
		 * 
		 * @param aKey a simple text key that represents the word within the description of the incident
		 * @param aValue a count of the word found across the different incidents
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void map(Text aKey, IntWritable aValue, Context aContext) throws IOException, InterruptedException {

			// @formatter:off
			/*
			 * Expects a line of input like the following: 
			 * 		[KEY(Text(Description_Word))  VALUE(IntWritable(<WordCount>))]
			 */
			// @formatter:on

			/*
			 * Swap and Write the Output Record.
			 */
			aContext.write(aValue, aKey);
		}
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class SimpleReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		/**
		 * Read all of the <key, List(values)> pairs generated by the Mapper<> and determine the frequency that a word
		 * occurs within the incident description words.
		 * <p>
		 * 
		 * @param aKey a IntWritable key that represents the word count of the incident description words
		 * @param aValues an iterable array of incident description words with identical counts found across the values
		 *            for the key
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void reduce(IntWritable aKey, Iterable<Text> aValues, Context aContext) throws IOException,
				InterruptedException {

			/*
			 * Loop through all of the values collected for each key (WordCount) from the Mapper<> and add each value
			 * (Description_Word) to the output as a separate <key, value> pair.
			 */
			for (Text myValue : aValues) {
				// @formatter:off
				/*
				 * Write the output record in the following format: 
				 * 		[KEY(IntWritable(<WordCount>)  List(VALUE(Text(Description_Word)))]
				 */
				// @formatter:on
				aContext.write(aKey, myValue);
			}
		}
	}

	/**
	 * @version 1.0, October 6, 2014
	 **/
	public static class DescendingIntComparator extends WritableComparator {

		public DescendingIntComparator() {
			super(IntWritable.class);
		}

		/**
		 * Perform the inverse of the normal compare routine (multiply by -1) in order to implement a descending sort
		 * implementation for IntWritable's.
		 **/
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
			return v1.compareTo(v2) * (-1);
		}
	}

	/**
	 * Setup the environment so that we can invoke the Mapper as a mapper-only job to perform the simple filtering of
	 * the initial data.
	 * 
	 * @param aArguments the arguments that were passed into the program
	 **/
	@Override
	public int run(String[] aArguments) throws Exception {
		final String usageText = "Usage: SwapWordCount <input_dir> <output_dir> [-descending]";

		if (aArguments.length < 2) {
			LOG.info(usageText);
			return -1;
		}

		Job job = new Job(getConf());

		FileInputFormat.setInputPaths(job, new Path(aArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(aArguments[1]));

		/*
		 * Process any additional arguments passed in...
		 */
		for (int i = 2; i < aArguments.length; i++) {
			if ("-descending".equals(aArguments[i])) {
				job.getConfiguration().setBoolean("wordcount.order.descending", true);
				LOG.info("Enabled descending ordering over the word counts.");
			} else {
				System.err.println("ERROR: Invalid argument : '" + aArguments[i] + "'");
				LOG.info(usageText);
				return -1;
			}
		}

		job.setJarByClass(SwapWordCount.class);
		job.setJobName("Swap WordCount against the NEISS Descriptions");

		job.setMapperClass(SwapCountMapper.class);
		job.setReducerClass(SimpleReducer.class);
		if (job.getConfiguration().getBoolean("wordcount.order.descending", false)) {
			job.setSortComparatorClass(DescendingIntComparator.class);
		}

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		/*
		 * Run the job and wait for it to be completed.
		 */
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	/**
	 * The main method calls the ToolRunner.run method, which in turn calls an options parser that interprets Hadoop
	 * command-line options and puts them into a Configuration object.
	 * 
	 * @param aArguments the arguments that were passed into the program
	 **/
	public static void main(String[] aArguments) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SwapWordCount(), aArguments);
		System.exit(exitCode);
	}
}
