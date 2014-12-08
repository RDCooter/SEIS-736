package edu.stthomas.gps.project;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Robert Driesch - UST Id# 101058113
 * @version 1.0, December 1, 2014
 **/
public class SortByYearCaseNbr extends Configured implements Tool {
	private static final Integer TOTAL_NUMBER_YEARS_OF_DATA = new Integer(17); // 1997 - 2013

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class YearPartitioner extends Partitioner<Text, Text> {
		private static final int LAST_YEAR_WITH_DATA = 2013;

		// @formatter:off
		/**
		 * Read the <key, value> pair generated by the Mapper and use the year in the TreatmentDate to determine which
		 * of the Reduce Tasks this record should be routed towards. The goal is to have the data from each year routed
		 * to a different reducer so when TOTAL_NUMBER_YEARS_OF_DATA Reduce Tasks are used, then each task should only
		 * be processing records that match the same year.
		 * <p>
		 * 
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
		 * <p>
		 * 
		 * @param aKey a simple text key that represents the CaseNbr
		 * @param aValue a tab delimited text string with the first field as the TreatmentDate
		 * @param aNumReduceTasks the number of Reduce Tasks allocated for this process
		 **/
		// @formatter:on
		@Override
		public int getPartition(Text aKey, Text aValue, int aNumReduceTasks) {
			/*
			 * Perform a sanity test to avoid any divide by zero exceptions (modulus with zero) when the number of
			 * reducers is set to zero for some reason.
			 */
			if (aNumReduceTasks == 0) {
				return 0;
			}

			/*
			 * Get the TreatmentDate from the tab delimited value in the <key,value> pair so we can extract out the year
			 * to use for the partitioning.
			 */
			String treatmentDate = aValue.toString().split("\t")[0];
			int yearInt = Integer.parseInt(treatmentDate.split("/")[2]); // Date in MDY format with "/" as the separator

			/*
			 * Set the partition number based upon the year of the incident (TreatmentDate) in order to keep the output
			 * results grouped based upon year without effecting the implicit ordering of the CaseNbr key by the
			 * map-reduce logic.
			 * 
			 * The calculation below will place the earliest year (1997) in reducer 0 and the last one (2013) in reducer
			 * 16.
			 */
			return (yearInt - LAST_YEAR_WITH_DATA + TOTAL_NUMBER_YEARS_OF_DATA - 1) % aNumReduceTasks;
		}
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class SimpleMapper extends Mapper<Text, Text, Text, Text> {

		/**
		 * Simple mapper that will basically just read the filtered product file which is keyed by the CaseNbr and send
		 * the records to the correct reducer partitioned by the year of the TreatmentDate to keep the entries grouped
		 * by year and sorted by the CaseNbr within each year.
		 * 
		 * @param aKey a simple text key that represents the CaseNbr
		 * @param aValue a tab delimited text string with the first field as the TreatmentDate
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void map(Text aKey, Text aValue, Context aContext) throws IOException, InterruptedException {

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
			 * Write the output record.
			 */
			aContext.write(aKey, aValue);
		}
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class SimpleReducer extends Reducer<Text, Text, Text, Text> {

		/**
		 * Read all of the <key, List(values)> pairs generated by the Mapper<> and separate the list of values back into
		 * new records for each value in the list. This will have the effect of sorting/grouping all of the keys
		 * together and still keeping the value as a separate entry in the result file.
		 * 
		 * @param aKey a simple text key that represents the CaseNbr
		 * @param aValues a list of text strings matching the CaseNbr
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void reduce(Text aKey, Iterable<Text> aValues, Context aContext) throws IOException,
				InterruptedException {

			/*
			 * Loop through all of the values collected for each key (CaseNbr) from the Mapper<> and add each value to
			 * the output as a separate <key, value> pair.
			 */
			for (Text myValue : aValues) {
				// @formatter:off
				/*
				 * Write the output record in the following format: 
				 * 		[KEY(Text(CaseNbr))	VALUE(Text(TreatmentDate
				 * 										\tHospital
				 * 										\tWeight
				 * 										\tStratum
				 * 										\tAge
				 * 										\tGender
				 * 										\tRace
				 * 										\tDiagnosis
				 *										\tBodyPart
				 * 										\tDisposition
				 * 										\tLocation
				 * 										\tProducts
				 * 										\tDescription))]
				 */
				// @formatter:on
				aContext.write(aKey, myValue);
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

		if (aArguments.length < 2) {
			System.out.printf("Usage: SortByYearCaseNbr <input_dir> <output_dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(SortByYearCaseNbr.class);
		job.setJobName("Sort Filtered NEISS Data by Year & CaseNbr");

		FileInputFormat.setInputPaths(job, new Path(aArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(aArguments[1]));

		job.setMapperClass(SimpleMapper.class);
		job.setReducerClass(SimpleReducer.class);
		job.setPartitionerClass(YearPartitioner.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(TOTAL_NUMBER_YEARS_OF_DATA.intValue());

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
		int exitCode = ToolRunner.run(new Configuration(), new SortByYearCaseNbr(), aArguments);
		System.exit(exitCode);
	}
}