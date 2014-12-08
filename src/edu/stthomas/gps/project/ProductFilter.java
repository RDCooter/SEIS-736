package edu.stthomas.gps.project;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * @author Robert Driesch - UST Id# 101058113
 * @version 1.0, December 1, 2014
 **/
public class ProductFilter extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ProductFilter.class);

	private static final String NAME_PREFIX = "/user/training/NEISS_Local/";
	private static final String GENDER_DATA = new String("Gender.dat");
	private static final String RACE_DATA = new String("Race.dat");
	private static final String DIAGNOSIS_DATA = new String("Diagnosis.dat");
	private static final String BODY_PART_DATA = new String("BodyPart.dat");
	private static final String DISPOSITION_DATA = new String("Disposition.dat");
	private static final String LOCATION_DATA = new String("Location.dat");

	public static enum NEISS_DATA {
		NUM_AMUSEMENTS, NUM_REJECTED, BAD_RECORD
	}

	/**
	 * @version 1.0, December 1, 2014
	 **/
	public static class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final String TAB_DELIMITER = new String("\t");
		private static final int SCHEMA_SIZE = 19;
		private static final Integer GENDER_UNKNOWN = new Integer(0);
		private static final Integer RACE_UNKNOWN = new Integer(0);
		private static final Integer RACE_OTHER = new Integer(3);
		private static final Integer DIAGNOSIS_UNKNOWN = new Integer(70);
		private static final Integer DIAGNOSIS_OTHER = new Integer(71);
		private static final Integer BODYPART_UNKNOWN = new Integer(87);
		private static final Integer DISPOSITION_UNKNOWN = new Integer(9);
		private static final Integer LOCATION_UNKNOWN = new Integer(0);

		// @formatter:off
		/*
		 * Build a sorted array of all of the possible Product Codes that may have been used to code for an inflatable
		 * amusement. Will perform a broad filtering based upon these codes and then use a TF-IDF search to find the
		 * ones whose description matches our keywords.
		 * 
		 * 		1200	=	Sports and Recreational Activity, Not Elsewhere Classified
		 * 		1242	=	Slides or Sliding Boards
		 * 		1293	=	Amusement Attractions
		 * 		3219	=	Other Playground Equipment
		 * 		3293	=	Water Slides, Other and Not Specified
		 * 		3294	=	Water Slides, Backyard/Home
		 * 		3295	=	Water Slides, Public
		 * 
		 */
		// @formatter:on
		private static final int[] AMUSEMENT_ATTRACTION_CODES = new int[] { 1200, 1242, 1293, 3219, 3293, 3294, 3295 };

		/*
		 * Local Cache Variables for the <key, value> for reuse for each input record being processed.
		 */
		private Text xTextKey = new Text();
		private Text xTextValue = new Text();

		/*
		 * Local Hash Tables to help Process the Distributed Cache information needed to process the records.
		 */
		private Map<Integer, String> genderMap = new HashMap<Integer, String>();
		private Map<Integer, String> raceMap = new HashMap<Integer, String>();
		private Map<Integer, String> diagnosisMap = new HashMap<Integer, String>();
		private Map<Integer, String> bodyPartMap = new HashMap<Integer, String>();
		private Map<Integer, String> dispositionMap = new HashMap<Integer, String>();
		private Map<Integer, String> locationMap = new HashMap<Integer, String>();

		/**
		 * Setup all of the local data structures required to process the raw NEISS data and expand the codes with their
		 * full text descriptions.
		 * 
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void setup(Context aContext) throws java.io.IOException, InterruptedException {

			// Loop through all of the cached files and populate the local data structures with their contents.
			Path[] allCachedFiles = DistributedCache.getLocalCacheFiles(aContext.getConfiguration());
			for (Path cachedFile : allCachedFiles) {

				/*
				 * Populate the local data structure for the distributed Race Description data.
				 */
				if (cachedFile.getName().equals(GENDER_DATA)) {
					parseDistributedFile(cachedFile, genderMap);
				}

				/*
				 * Populate the local data structure for the distributed Race Description data.
				 */
				else if (cachedFile.getName().equals(RACE_DATA)) {
					parseDistributedFile(cachedFile, raceMap);
				}

				/*
				 * Populate the local data structure for the distributed Diagnosis Description data.
				 */
				else if (cachedFile.getName().equals(DIAGNOSIS_DATA)) {
					parseDistributedFile(cachedFile, diagnosisMap);
				}

				/*
				 * Populate the local data structure for the distributed Diagnosis Description data.
				 */
				else if (cachedFile.getName().equals(BODY_PART_DATA)) {
					parseDistributedFile(cachedFile, bodyPartMap);
				}

				/*
				 * Populate the local data structure for the distributed Diagnosis Description data.
				 */
				else if (cachedFile.getName().equals(DISPOSITION_DATA)) {
					parseDistributedFile(cachedFile, dispositionMap);
				}

				/*
				 * Populate the local data structure for the distributed Diagnosis Description data.
				 */
				else if (cachedFile.getName().equals(LOCATION_DATA)) {
					parseDistributedFile(cachedFile, locationMap);
				}
			}

			/*
			 * Make sure that all of the local data structures were populated and that all of the distributed data was
			 * replicated successfully.
			 */
			if (genderMap.isEmpty()) {
				throw new IOException("Unable to load Gender Description data [" + GENDER_DATA + "].");
			}
			if (raceMap.isEmpty()) {
				throw new IOException("Unable to load Race Description data [" + RACE_DATA + "].");
			}
			if (diagnosisMap.isEmpty()) {
				throw new IOException("Unable to load Diagnosis Description data [" + DIAGNOSIS_DATA + "].");
			}
			if (bodyPartMap.isEmpty()) {
				throw new IOException("Unable to load BodyPart Description data [" + BODY_PART_DATA + "].");
			}
			if (dispositionMap.isEmpty()) {
				throw new IOException("Unable to load Disposition data [" + DISPOSITION_DATA + "].");
			}
			if (locationMap.isEmpty()) {
				throw new IOException("Unable to load Location data [" + LOCATION_DATA + "].");
			}
		}

		/**
		 * Read the raw NEISS data and perform the pre-processing and filtering of the data to massage the data into a
		 * workable form for the remainder of the MapReduce algorithms.
		 * <p>
		 * 
		 * @param aKey a simple numeric key (offset) whose value is not consumed
		 * @param aValue a tab delimited text string which contains the NEISS data
		 * @param aContext the context object associated with this process
		 **/
		@Override
		protected void map(LongWritable aKey, Text aValue, Context aContext) throws IOException, InterruptedException {
			Integer lookupKey;
			String lookupDescription;

			// @formatter:off
			/*
			 * Expects a line of input like the following: 
			 * 		[KEY(LongWriteable(1))	VALUE(Text(CaseNbr
			 * 											\tTreatmentDate
			 * 											\tHospital
			 * 											\tWeight
			 * 											\tStratum
			 * 											\tAge
			 * 											\tGender
			 * 											\tRace
			 * 											\tRaceOther
			 * 											\tDiagnosis
			 * 											\tDiagnosisOther
			 *							 				\tBodyPart
			 * 											\tDisposition
			 * 											\tLocation
			 * 											\tFireDept
			 * 											\tProduct1
			 * 											\tProduct2
			 * 											\tDescription1
			 * 											\tDescription2))]
			 */
			 // @formatter:on

			/*
			 * Convert the line, which is received as a Text object into a String object that we can manipulate better.
			 */
			String myLine = aValue.toString();

			/*
			 * Perform some simple validation checking upon the raw NEISS data to ensure that it is not corrupted and
			 * will screw up any of the the calculations that follow.
			 */
			if (myLine.contains(TAB_DELIMITER)) {
				/*
				 * Split the line of raw NEISS data into its (at most) 19 separate fields in order to grab the value of
				 * interest. We do check the number of split fields as an invalid number will indicate a record that
				 * does not match the schema. However, we will be a bit lenient with the last field (Description2) as it
				 * appears that this field was not entered into the data set consistently when it was not required (e.g.
				 * for a short description).
				 */
				String[] dataVals = myLine.split(TAB_DELIMITER, SCHEMA_SIZE);

				// Test for a valid amusement product code. Note that it can't be more than 19.
				if (dataVals.length >= (SCHEMA_SIZE - 1) && isAmusementProductCode(dataVals[15], dataVals[16])) {
					// @formatter:off
					/*
					 * Rebuild the Text Value so that we can write the output record in the following format: 
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

					/*
					 * Identify the unique CPSC Case Number associated with this output record. Will also perform some
					 * simple cleansing of the value to ensure that it is truly a nine character zero padded value.
					 */
					try {
						xTextKey.set("000000000".substring(dataVals[0].length()) + dataVals[0]);
					} catch (StringIndexOutOfBoundsException e) {
						System.err.println("ERROR: dataVals.size=" + dataVals.length + " dataVals[0]=\"" + dataVals[0]
								+ "\" Length=" + dataVals[0].length() + " : " + StringUtils.stringifyException(e));
					}

					/*
					 * Build up a string of all of the original raw NEISS data with a tab delimiter, but clean-up some
					 * of the separated data into a single field.
					 */
					StringBuilder valueBuilder = new StringBuilder();
					valueBuilder.append(dataVals[1]); // Initialize with the Treatment Date

					/*
					 * Add the Statistical information (Hospital, Weight and Stratum) back into the output record.
					 */
					valueBuilder.append(TAB_DELIMITER).append(dataVals[2]).append(TAB_DELIMITER).append(dataVals[3])
							.append(TAB_DELIMITER).append(dataVals[4]);

					/*
					 * Add the Age information back into the output record.
					 */
					valueBuilder.append(TAB_DELIMITER).append(dataVals[5]);

					/*
					 * Add the Gender information back into the output record.
					 */
					try {
						lookupKey = (dataVals[6].isEmpty()) ? GENDER_UNKNOWN : Integer.parseInt(dataVals[6]);
					} catch (NumberFormatException e) {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);
						lookupKey = GENDER_UNKNOWN; // Use the default.

						System.err.println("ERROR: CaseNbr=" + xTextKey.toString() + " TreatmentDate=" + dataVals[1]
								+ " dataVals[6]=\"" + dataVals[6] + "\" Length=" + dataVals[6].length());
						System.err.println("BAD_DATA:  \"" + myLine + "\" : " + StringUtils.stringifyException(e));
					}
					valueBuilder.append(TAB_DELIMITER).append(genderMap.get(lookupKey));

					/*
					 * Add the Race information back into the output record and compress the Race and the RaceOther
					 * description into a single value.
					 */
					try {
						lookupKey = (dataVals[7].isEmpty()) ? RACE_UNKNOWN : Integer.parseInt(dataVals[7]);
					} catch (NumberFormatException e) {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);
						lookupKey = RACE_UNKNOWN; // Use the default.

						System.err.println("ERROR: CaseNbr=" + xTextKey.toString() + " dataVals[7]=\"" + dataVals[7]
								+ "\" Length=" + dataVals[7].length());
						System.err.println("ERROR: TreatmentDate=" + dataVals[1] + " dataVals[8]=\"" + dataVals[8]
								+ "\" Length=" + dataVals[8].length());
						System.err.println("BAD_DATA:  \"" + myLine + "\" : " + StringUtils.stringifyException(e));
					}
					lookupDescription = raceMap.get(lookupKey);
					valueBuilder.append(TAB_DELIMITER).append(
							(lookupKey.compareTo(RACE_OTHER) != 0) ? lookupDescription
									: (lookupDescription + "-" + dataVals[8].toUpperCase()));

					/*
					 * Add the Diagnosis information back into the output record and compress the Diagnosis and the
					 * DiagnosisOther description into a single value.
					 */
					try {
						lookupKey = (dataVals[9].isEmpty()) ? DIAGNOSIS_UNKNOWN : Integer.parseInt(dataVals[9]);
					} catch (NumberFormatException e) {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);
						lookupKey = DIAGNOSIS_UNKNOWN; // Use the default.

						// Throw some bread crumbs into the logs to help diagnosis the bad data issue.
						System.err.println("ERROR: CaseNbr=" + xTextKey.toString() + " dataVals[9]=\"" + dataVals[9]
								+ "\" Length=" + dataVals[9].length());
						System.err.println("ERROR: TreatmentDate=" + dataVals[1] + " dataVals[10]=\"" + dataVals[10]
								+ "\" Length=" + dataVals[10].length());
						System.err.println("BAD_DATA:  \"" + myLine + "\" : " + StringUtils.stringifyException(e));
					}
					lookupDescription = diagnosisMap.get(lookupKey);
					valueBuilder.append(TAB_DELIMITER).append(
							(lookupKey.compareTo(DIAGNOSIS_OTHER) != 0) ? lookupDescription
									: (lookupDescription + "-" + dataVals[10].toUpperCase()));

					/*
					 * Add the BodyPart information back into the output record.
					 */
					try {
						lookupKey = (dataVals[11].isEmpty()) ? BODYPART_UNKNOWN : Integer.parseInt(dataVals[11]);
					} catch (NumberFormatException e) {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);
						lookupKey = BODYPART_UNKNOWN; // Use the default.

						System.err.println("ERROR: CaseNbr=" + xTextKey.toString() + " TreatmentDate=" + dataVals[1]
								+ " dataVals[11]=\"" + dataVals[11] + "\" Length=" + dataVals[11].length());
						System.err.println("BAD_DATA:  \"" + myLine + "\" : " + StringUtils.stringifyException(e));
					}
					valueBuilder.append(TAB_DELIMITER).append(bodyPartMap.get(lookupKey));

					/*
					 * Add the Disposition information back into the output record.
					 */
					try {
						lookupKey = (dataVals[12].isEmpty()) ? DISPOSITION_UNKNOWN : Integer.parseInt(dataVals[12]);
					} catch (NumberFormatException e) {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);
						lookupKey = DISPOSITION_UNKNOWN; // Use the default.

						System.err.println("ERROR: CaseNbr=" + xTextKey.toString() + " TreatmentDate=" + dataVals[1]
								+ " dataVals[12]=\"" + dataVals[12] + "\" Length=" + dataVals[12].length());
						System.err.println("BAD_DATA:  \"" + myLine + "\" : " + StringUtils.stringifyException(e));
					}
					valueBuilder.append(TAB_DELIMITER).append(dispositionMap.get(lookupKey));

					/*
					 * Add the Location information back into the output record.
					 */
					try {
						lookupKey = (dataVals[13].isEmpty()) ? LOCATION_UNKNOWN : Integer.parseInt(dataVals[13]);
					} catch (NumberFormatException e) {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);
						lookupKey = LOCATION_UNKNOWN; // Use the default.

						System.err.println("ERROR: CaseNbr=" + xTextKey.toString() + " TreatmentDate=" + dataVals[1]
								+ " dataVals[13]=\"" + dataVals[13] + "\" Length=" + dataVals[13].length());
						System.err.println("BAD_DATA:  \"" + myLine + "\" : " + StringUtils.stringifyException(e));
					}
					valueBuilder.append(TAB_DELIMITER).append(locationMap.get(lookupKey));

					/*
					 * Add the Product Identifiers (both of them) back into the output record and compress the Product1
					 * and Product2 into a single value.
					 */
					valueBuilder.append(TAB_DELIMITER).append(dataVals[15] + " " + dataVals[16]);

					/*
					 * Add the Description (lines 1 and 2) back into the output record and compress the Description1 and
					 * Description2 into a single value.
					 */
					valueBuilder.append(TAB_DELIMITER).append(
							dataVals[17] + ((dataVals.length == SCHEMA_SIZE) ? dataVals[18] : ""));
					xTextValue.set(valueBuilder.toString());

					/*
					 * Write the output record.
					 */
					aContext.write(xTextKey, xTextValue);
					aContext.getCounter(NEISS_DATA.NUM_AMUSEMENTS).increment(1);
				} else {
					if (dataVals.length >= (SCHEMA_SIZE - 1)) {
						// Bump the counter of rejected records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.NUM_REJECTED).increment(1);
					} else {
						// Bump the counter of bad records that were encountered during the processing.
						aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);

						// This is a case where the record does not have the expected number of tab delimited fields
						// found within the String, then this must be an invalid line of data.
						System.err.println("BAD_DATA:  \"" + myLine + "\"");
						System.err.println("ERROR:  Found a record with " + dataVals.length
								+ " tab deliminted field. Expected 18 or 19 for this schema!");
					}
				}
			} else {
				// Bump the counter of bad records that were encountered during the processing.
				aContext.getCounter(NEISS_DATA.BAD_RECORD).increment(1);

				// Since there are no known delimiters (tabs) found within the String, then this must be an invalid
				// line of data. We will spit the record out to STDERR and ignore it for the remainder of the
				// processing.
				System.err.println("ERROR:  Found a record not formatted correctly for the schema!");
				System.err.println("BAD_DATA:  \"" + myLine + "\"");
			}
		}

		/**
		 * @param aFirstProductCode The first (primary) product code listed in the NEISS record. This value should
		 *            always be set
		 * @param aSecondProductCode The second product code listed in the NEISS record. This value may not always be
		 *            set
		 * @return A boolean that indicates if either of the product codes contain a match to ones of the identified
		 *         amusement codes
		 */
		protected boolean isAmusementProductCode(String aFirstProductCode, String aSecondProductCode) {
			boolean result = false;
			try {
				/*
				 * Note: A value of (-<insertion-point> - 1) is returned when the Arrays.binarySearch() does not find
				 * any match during its probe where the <insertion-point> is defined as the location within the array
				 * where this missing would have been inserted.
				 */
				if (Arrays.binarySearch(AMUSEMENT_ATTRACTION_CODES, Integer.parseInt(aFirstProductCode)) >= 0)
					result = true;
				else if (Arrays.binarySearch(AMUSEMENT_ATTRACTION_CODES, Integer.parseInt(aSecondProductCode)) >= 0)
					result = true;
			} catch (NumberFormatException e) {
				// Do nothing, already initialized with a default value of false.
			}
			return result;
		}

		/**
		 * Read the specified distributed cache file and populate the local data structure with the contents.
		 * 
		 * @param aPath the path to the distributed cache file to use
		 * @param aCacheMap the local data structure to be updated with the contents of the file
		 **/
		private void parseDistributedFile(Path aPath, Map<Integer, String> aCacheMap) throws IOException {
			LOG.info("Added file \"" + aPath.getName().toString() + "\" from the distributed cache.");
			BufferedReader bufferedRdr = new BufferedReader(new FileReader(aPath.toString()));
			try {
				String lineOfData = bufferedRdr.readLine();
				while (lineOfData != null) {
					String[] lineTokens = lineOfData.split(TAB_DELIMITER);
					aCacheMap.put(Integer.parseInt(lineTokens[0]), lineTokens[1]);
					lineOfData = bufferedRdr.readLine();
				}
			} catch (IOException ioe) {
				System.err.println("ERROR: Caught exception while parsing the cached file '"
						+ aPath.getName().toString() + "' : " + StringUtils.stringifyException(ioe));
			} finally {
				LOG.info(aCacheMap.size() + " entries added from distributed cache file \""
						+ aPath.getName().toString() + "\" to local data structure.");
				bufferedRdr.close();
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
			System.out.printf("Usage: ProductFilter <input_dir> <output_dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(ProductFilter.class);
		job.setJobName("Filter NEISS Data by Products");

		FileInputFormat.setInputPaths(job, new Path(aArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(aArguments[1]));

		job.setMapperClass(ProductMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		/*
		 * Set the number of reduce tasks to be used for this execution. This value is set to zero since we are only
		 * performing some simple filtering and cleansing of the initial data.
		 */
		job.setNumReduceTasks(0);

		/*
		 * Add all of the files that we need to have distributed for each of the mappers to take advantage of.
		 */
		DistributedCache.addCacheFile(new URI(NAME_PREFIX + GENDER_DATA), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(NAME_PREFIX + RACE_DATA), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(NAME_PREFIX + DIAGNOSIS_DATA), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(NAME_PREFIX + BODY_PART_DATA), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(NAME_PREFIX + DISPOSITION_DATA), job.getConfiguration());
		DistributedCache.addCacheFile(new URI(NAME_PREFIX + LOCATION_DATA), job.getConfiguration());

		/*
		 * Run the job and wait for it to be completed.
		 */
		boolean success = job.waitForCompletion(true);

		/*
		 * Quickly output the local counters to the local output stream (console).
		 */
		Counters allCounters = job.getCounters();
		Counter myCounter = allCounters.findCounter(NEISS_DATA.NUM_AMUSEMENTS);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.NUM_REJECTED);
		LOG.info(myCounter.getDisplayName() + " : " + myCounter.getValue());
		myCounter = allCounters.findCounter(NEISS_DATA.BAD_RECORD);
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
		int exitCode = ToolRunner.run(new Configuration(), new ProductFilter(), aArguments);
		System.exit(exitCode);
	}
}
