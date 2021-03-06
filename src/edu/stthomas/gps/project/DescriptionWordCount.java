package edu.stthomas.gps.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * @author Robert Driesch - UST Id# 101058113
 * @version 1.0, December 1, 2014
 **/
public class DescriptionWordCount extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(DescriptionWordCount.class);

	private static final String TAB_DELIMITER = new String("\t");
	private static final String EQUALS_DELIMITER = new String("=");

	public static enum NEISS_DATA {
		TOAL_RECORDS_GENERATED
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class DescriptionWordCountMapper extends Mapper<Text, IntWritable, Text, Text> {

		/*
		 * Local Cache Variables for the <key,value> for reuse for each input record being processed.
		 */
		private Text xCaseNbrKey = new Text();
		private Text xWordCountValue = new Text();

		/**
		 * Read the Description Word Frequency data and re-swizzle the key so that it is only made up of the CaseNbr and
		 * FileName so we can determine the total number of words within the Incident Description that will be needed as
		 * we calculate the TF-IDF.
		 * <p>
		 * 
		 * @param aKey a composite text key that represents the DescriptionWord as well as the CaseNbr/FileName where
		 *            the word originates
		 * @param aValue the count of the number of times that this word occurs within the incident description
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void map(Text aKey, IntWritable aValue, Context aContext) throws IOException, InterruptedException {

			// @formatter:off
			/*
			 * Expects a line of input like the following: 
			 * 				[KEY(Text(<Description_Word \t CaseNbr@FileName>))  VALUE(IntWritable(<WordCount>))]
			 */
			 // @formatter:on

			/*
			 * Convert the data, which is received as a Text object into a String and Integer objects that we can
			 * manipulate better.
			 */
			String[] myKey = aKey.toString().split(TAB_DELIMITER);
			Integer myValue = aValue.get();

			// @formatter:off
			/*
			 * Write the output record in the following format: 
			 * 		[KEY(Text(CaseNbr@FileName))  VALUE(Text(<Description_Word=WordCount>))]
			 */
			// @formatter:on
			xCaseNbrKey.set(myKey[1]);
			xWordCountValue.set(myKey[0] + EQUALS_DELIMITER + myValue.toString());
			aContext.write(xCaseNbrKey, xWordCountValue);
		}
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class SumDescriptionWordCountReducer extends Reducer<Text, Text, Text, Text> {

		/*
		 * Local Cache Variables for the <key, value> for reuse for each output reduce record being processed.
		 */
		private Text xTextKey = new Text();
		private Text xTextValues = new Text();

		private Map<String, Integer> xAllDescriptionWords = new HashMap<String, Integer>();

		/**
		 * Read all of the <key, List(values)> pairs generated by the Mapper<> and determine the number of times that we
		 * encounter a word as it occurs within the description of the different incidents and group those counts based
		 * upon the word (key).
		 * <p>
		 * 
		 * @param aKey a Text value key that represents the CaseNbr/FileName associated with the incident description
		 * @param aValues an iterable array of a composite value including the DescriptionWord and the count of the word
		 *            frequency found within the incident
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void reduce(Text aKey, Iterable<Text> aValues, Context aContext) throws IOException,
				InterruptedException {

			/*
			 * Make sure that the temporary data structure that we use to hold the Description Words as we sum up the
			 * totals gets cleared between different sets of iterable data.
			 */
			xAllDescriptionWords.clear();

			/*
			 * Convert the key which is received as a Text object into a String object that we can manipulate better.
			 */
			String myOriginalKey = aKey.toString();

			/**
			 * Loop through all of the values collected for each key (CaseNbr@FileName) from the Mapper<> that have the
			 * pattern list like this: List[Text(<Description_Word=WordCount>))]
			 * <p>
			 * 
			 * Split apart the DescriptionWord from the WordCount of that word so that we can summarize the total of all
			 * of the WordCounts within this iterable list. Store the information needed to write this record back out
			 * to the context into a temporary data structure that we can process after all of the iterable values have
			 * been read.
			 */
			int sumAllWordCounts = 0;
			for (Text myValue : aValues) {
				/*
				 * Split the DescriptionWord from the WordCount using the equals inserted by the Mapper<> as a delimiter
				 * and write the values into the temporary data structure.
				 */
				String[] wordAndCounter = myValue.toString().split(EQUALS_DELIMITER);
				xAllDescriptionWords.put(wordAndCounter[0], Integer.valueOf(wordAndCounter[1]));
				sumAllWordCounts += Integer.parseInt(wordAndCounter[1]);
			}

			/*
			 * Now that we have processed all of the iterable values and generated the sum of all of the
			 * DescriptionWords for this incident, we can start to unload all of the data that we have saved away in the
			 * temporary data structure and write the formatted records back out to the context.
			 */
			for (String eachWordKey : xAllDescriptionWords.keySet()) {
				// @formatter:off
				/*
				 * Write the output record in the following format: 
				 * 		[KEY(Text(<Description_Word = CaseNbr@FileName>))  VALUE(Text(<WordCount/SUM(AllWordCounts)>))]
				 */
				// @formatter:on
				xTextKey.set(eachWordKey + EQUALS_DELIMITER + myOriginalKey);
				xTextValues.set(xAllDescriptionWords.get(eachWordKey) + "/" + sumAllWordCounts);
				aContext.write(xTextKey, xTextValues);
				aContext.getCounter(NEISS_DATA.TOAL_RECORDS_GENERATED).increment(1);
			}
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
		final String usageText = "Usage: DescriptionWordCount <input_dir> <output_dir>";

		/*
		 * Process any arguments passed in...
		 */
		if (aArguments.length < 2) {
			LOG.info(usageText);
			return -1;
		}

		Job job = new Job(getConf());

		FileInputFormat.setInputPaths(job, new Path(aArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(aArguments[1]));

		job.setJarByClass(DescriptionWordCount.class);
		job.setJobName("Total WordCount against the Incident Descriptions");

		job.setMapperClass(DescriptionWordCountMapper.class);
		job.setReducerClass(SumDescriptionWordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(24);

		/*
		 * Run the job and wait for it to be completed.
		 */
		boolean success = job.waitForCompletion(true);

		/*
		 * Quickly output the local counters to the local output stream (console).
		 */
		Counter myCounter = job.getCounters().findCounter(NEISS_DATA.TOAL_RECORDS_GENERATED);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		return success ? 0 : 1;
	}

	/**
	 * The main method calls the ToolRunner.run method, which in turn calls an options parser that interprets Hadoop
	 * command-line options and puts them into a Configuration object.
	 * 
	 * @param aArguments the arguments that were passed into the program
	 **/
	public static void main(String[] aArguments) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new DescriptionWordCount(), aArguments);
		System.exit(exitCode);
	}
}
