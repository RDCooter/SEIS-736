package edu.stthomas.gps.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author Robert Driesch - UST Id# 101058113
 * @version 1.0, December 1, 2014
 **/
public class DescriptionWordFrequency extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(DescriptionWordFrequency.class);

	private static final String NAME_PREFIX = "/user/training/NEISS_Local/";
	private static final String STOP_WORD_DATA = "StopWords.dat";

	public static enum NEISS_DATA {
		TOAL_RECORDS_PROCESSED, TOTAL_REJECTED_WORDS, NUM_REJECTED_ZEROWORDS, NUM_REJECTED_STOPWORDS, NUM_VALID_WORDS, NUM_REJECTED_DIGITS, NUM_REJECTED_NONCHARS
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class DescriptionWordFrequencyMapper extends Mapper<Text, Text, Text, IntWritable> {

		/*
		 * Local Cache Variables for the <key,value> for reuse for each input record being processed.
		 */
		private Text xWordKey = new Text();

		private boolean xCaseSensitive = false;
		private Set<String> patternsToSkip = new HashSet<String>();

		private static final IntWritable ONE_COUNT = new IntWritable(1);
		private static final String TAB_DELIMITER = new String("\t");
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		/**
		 * Read the filtered NEISS data and calculate the frequency of the appearance of words within the incident
		 * description using a simple WordCount algorithm.
		 * <p>
		 * 
		 * Since we are using this results of this MapReduce to calculate the TF-IDF, we are including both FileName and
		 * the CaseNbr associated with the DescriptionWords so that we can eventually get back to the original record.
		 * <p>
		 * 
		 * @param aKey a simple text key that represents the CaseNbr
		 * @param aValue a tab delimited text string with the last field as the incident Description
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void map(Text aKey, Text aValue, Context aContext) throws IOException, InterruptedException {

			/*
			 * Get the name of the file from the InputSplit stored away in the Context.
			 */
			String fileName = ((FileSplit) aContext.getInputSplit()).getPath().getName();

			// @formatter:off
			/*
			 * Expects a line of input like the following: 
			 * 				[KEY(Text(CaseNbr))	VALUE(Text(TreatmentDate
			 * 												\tHospital
			 *		 										\tWeight
			 * 												\tStratum
			 * 												\tAge
			 * 												\tGender
			 * 												\tRace
			 * 												\tDiagnosis
			 *												\tBodyPart
			 * 												\tDisposition
			 * 												\tLocation
			 * 												\tProducts
			 * 												\tDescription))]
			 */
			 // @formatter:on

			/*
			 * Convert the line of data, which is received as a Text object into a String object that we can manipulate
			 * better. Also determine if we should mono-case text to make the consolidation and summarization easier.
			 */
			aContext.getCounter(NEISS_DATA.TOAL_RECORDS_PROCESSED).increment(1);
			String myLine = aValue.toString();
			if (!xCaseSensitive) {
				myLine = myLine.toLowerCase();
			}

			/*
			 * Convert the key of this line back into a string so that we can we re-format it for the new <key,value>
			 * pair that we are generating.
			 */
			String myCaseNbr = aKey.toString();

			/*
			 * Split the line of NEISS tab delimited data into its (at most) 13 separate fields in order to grab the
			 * incident Description.
			 */
			StringBuilder textKeyBuilder = new StringBuilder();
			String descriptionLine = myLine.split(TAB_DELIMITER, 13)[12];
			for (String descriptionWord : WORD_BOUNDARY.split(descriptionLine)) {
				/*
				 * Check to see if the word within the description should be included into the final set of words or if
				 * it should be skipped (because of Stop-Word processing).
				 */
				if (descriptionWord.isEmpty() || !Character.isLetter(descriptionWord.charAt(0))
						|| Character.isDigit(descriptionWord.charAt(0)) || patternsToSkip.contains(descriptionWord)) {
					aContext.getCounter(NEISS_DATA.TOTAL_REJECTED_WORDS).increment(1);
					NEISS_DATA counterType;
					if (descriptionWord.isEmpty())
						counterType = NEISS_DATA.NUM_REJECTED_ZEROWORDS;
					else if (!Character.isLetter(descriptionWord.charAt(0)))
						counterType = NEISS_DATA.NUM_REJECTED_NONCHARS;
					else if (Character.isDigit(descriptionWord.charAt(0)))
						counterType = NEISS_DATA.NUM_REJECTED_DIGITS;
					else
						counterType = NEISS_DATA.NUM_REJECTED_STOPWORDS;
					aContext.getCounter(counterType).increment(1);
					continue;
				}

				/*
				 * Build up the Format of the New Key: "Description_Word\tCaseNbr@FileName"
				 */
				textKeyBuilder.delete(0, textKeyBuilder.length());// Clear first
				textKeyBuilder.append(descriptionWord);
				textKeyBuilder.append("\t");
				textKeyBuilder.append(myCaseNbr);
				textKeyBuilder.append("@");
				textKeyBuilder.append(fileName);

				// @formatter:off
				/*
				 * Write the output record in the following format: 
				 * 		[KEY(Text(<Description_Word \t CaseNbr@FileName>))  VALUE(IntWritable(1))]
				 */
				// @formatter:on
				xWordKey.set(textKeyBuilder.toString());
				aContext.write(xWordKey, ONE_COUNT);
				aContext.getCounter(NEISS_DATA.NUM_VALID_WORDS).increment(1);
			}
		}

		/**
		 * Setup all of the local data structures required to process the filtered NEISS data to generate the word
		 * frequency for the incident Description text.
		 * 
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void setup(Context aContext) throws IOException, InterruptedException {

			// Get the local configuration from the context so that we can test for the different options and extract
			// out any distributed files that maybe used.
			Configuration myConfig = aContext.getConfiguration();

			/*
			 * Test for Case Sensitivity usage while processing the incident Description words.
			 */
			xCaseSensitive = myConfig.getBoolean("wordcount.case.sensitive", false);

			/*
			 * Test for Stop Word usage while processing the incident Description words.
			 */
			if (myConfig.getBoolean("wordcount.skip.patterns", false)) {
				Path[] allCachedFiles = DistributedCache.getLocalCacheFiles(myConfig);
				parseSkipFile(allCachedFiles[0]);
			}
		}

		/**
		 * Parse all of the data stored within the distributed cache file and load it into the data structure of
		 * patterns to skip while generating the list of words for the <key,value> pair.
		 * 
		 * @param aPatternsPath the path to the distributed cache file to use
		 **/
		private void parseSkipFile(Path aPatternsPath) throws IOException {
			LOG.info("Added file \"" + aPatternsPath.getName().toString() + "\" from the distributed cache.");
			BufferedReader bufferedRdr = new BufferedReader(new FileReader(aPatternsPath.toString()));
			try {
				String patternLine = bufferedRdr.readLine();
				while (patternLine != null) {
					if (!xCaseSensitive) {
						patternLine = patternLine.toLowerCase();
					}
					patternsToSkip.add(patternLine);
					patternLine = bufferedRdr.readLine();
				}
			} catch (IOException ioe) {
				System.err.println("ERROR: Caught exception while parsing the cached file '"
						+ aPatternsPath.getName().toString() + "' : " + StringUtils.stringifyException(ioe));
			} finally {
				LOG.info(patternsToSkip.size() + " entries added from distributed cache file \""
						+ aPatternsPath.getName().toString() + "\" to local data structure.");
				bufferedRdr.close();
			}
		}
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class SumFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		/*
		 * Local Cache Variables for the <key, value> for reuse for each output reduce record being processed.
		 */
		private IntWritable xSumIntWritable = new IntWritable();

		/**
		 * Read all of the <key, List(values)> pairs generated by the Mapper<> and determine the number of times that we
		 * encounter a word as it occurs within the description of the different incidents and group those counts based
		 * upon the word (key).
		 * <p>
		 * 
		 * @param aKey a Text value key that represents the word within the description of the incident
		 * @param aValues an iterable array of counts of the word found across the values for the key
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void reduce(Text aKey, Iterable<IntWritable> aValues, Context aContext) throws IOException,
				InterruptedException {

			/*
			 * Loop through all of the values collected for each key (<Description_Word\tCaseNbr@FileName>) from the
			 * Mapper<> and summarize the total number of times that the word was encountered within the data.
			 */
			int myWordCount = 0;
			for (IntWritable myValue : aValues) {
				myWordCount += myValue.get();
			}
			xSumIntWritable.set(myWordCount);

			// @formatter:off
			/*
			 * Write the output record in the following format: 
			 * 		[KEY(Text(<Description_Word \t CaseNbr@FileName>))  VALUE(IntWritable(SUM(Description_Word_Count)))]
			 */
			// @formatter:on
			aContext.write(aKey, xSumIntWritable);
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
		final String usageText = "Usage: DescriptionWordCount <input_dir> <output_dir> [-stopWords -caseSensitive]";

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
			if ("-stopWords".equals(aArguments[i])) {
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				DistributedCache.addCacheFile(new URI(NAME_PREFIX + STOP_WORD_DATA), job.getConfiguration());
				LOG.info("Added file to the distributed cache: " + NAME_PREFIX + STOP_WORD_DATA);
			} else if ("-caseSensitive".equals(aArguments[i])) {
				job.getConfiguration().setBoolean("wordcount.case.sensitive", true);
				LOG.info("Enabled case sensitivity while processing the incident Description words.");
			} else {
				System.err.println("ERROR: Invalid argument : '" + aArguments[i] + "'");
				LOG.info(usageText);
				return -1;
			}
		}

		job.setJarByClass(DescriptionWordFrequency.class);
		job.setJobName("WordFrequency against the NEISS Descriptions");

		job.setMapperClass(DescriptionWordFrequencyMapper.class);
		job.setReducerClass(SumFrequencyReducer.class);
		job.setCombinerClass(SumFrequencyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setNumReduceTasks(24);

		/*
		 * Run the job and wait for it to be completed.
		 */
		boolean success = job.waitForCompletion(true);

		/*
		 * Quickly output the local counters to the local output stream (console).
		 */
		Counters allCounters = job.getCounters();
		Counter myCounter = allCounters.findCounter(NEISS_DATA.TOAL_RECORDS_PROCESSED);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.NUM_VALID_WORDS);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.TOTAL_REJECTED_WORDS);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.NUM_REJECTED_ZEROWORDS);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.NUM_REJECTED_STOPWORDS);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.NUM_REJECTED_DIGITS);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.NUM_REJECTED_NONCHARS);
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
		int exitCode = ToolRunner.run(new Configuration(), new DescriptionWordFrequency(), aArguments);
		System.exit(exitCode);
	}
}
