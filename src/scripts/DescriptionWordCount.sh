#!/bin/bash
# =========================================================
# This script will run multiple MapReduce jobs with the 
# goal of performing word count processing against the 
# incident Descriptions for filtered NEISS Data.
#
# USAGE:  DescriptionWordCount.sh
#
# =========================================================

# =========================================================
#
# Define & Initialize any Environment Variables
#
# =========================================================

# ---------------------------------------------------------
# Define Global Variables
# ---------------------------------------------------------
SCRIPT_NAME=`(basename ${0} .sh)`
HADOOP="$( which hadoop )"
LOCAL_PATH="/home/training/Project"
HDFS_PATH="/user/training"
LOG_FILE="${LOCAL_PATH}/Logs/${SCRIPT_NAME}_`date +%s`.log"

# ---------------------------------------------------------
# Define Jar File Definitions for each of the jobs.
# ---------------------------------------------------------
MAPREDUCE_JAR_JOB1="NEISSProductIncidents.jar"
MAPREDUCE_JAR_JOB2="NEISSProductIncidents.jar"
MAPREDUCE_JAR_JOB3="NEISSProductIncidents.jar"

# ---------------------------------------------------------
# Define the Driver Class Definitions for each Jar File.
# ---------------------------------------------------------
MAIN_CLASS_JOB1="edu.stthomas.gps.project.DescriptionWordFrequency"
MAIN_CLASS_JOB2="edu.stthomas.gps.project.DescriptionWordCount"
MAIN_CLASS_JOB3="edu.stthomas.gps.project.DescriptionTFIDF"

# ---------------------------------------------------------
# Define the Directories needed for the multiple jobs.
# ---------------------------------------------------------
INPUT_DIRECTORY="${HDFS_PATH}/NEISS_InitialFilter"
INTERMEDIATE_DIRECTORY1="${HDFS_PATH}/NEISS_Frequency"
INTERMEDIATE_DIRECTORY2="${HDFS_PATH}/NEISS_WordCounts"
OUTPUT_DIRECTORY="${HDFS_PATH}/NEISS_TFIDF"

# ---------------------------------------------------------
# Define the Execution Commands for each job.
# ---------------------------------------------------------
JOB_1_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB1} ${MAIN_CLASS_JOB1} ${INPUT_DIRECTORY} ${INTERMEDIATE_DIRECTORY1} -stopWords"
JOB_2_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB2} ${MAIN_CLASS_JOB2} ${INTERMEDIATE_DIRECTORY1} ${INTERMEDIATE_DIRECTORY2}"
JOB_3_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB3} ${MAIN_CLASS_JOB3} ${INTERMEDIATE_DIRECTORY2} ${OUTPUT_DIRECTORY} -totalIncidents 461923"
CLEANUP_CMD="${HADOOP} fs -rm -r ${INTERMEDIATE_DIRECTORY1} ${INTERMEDIATE_DIRECTORY2} ${OUTPUT_DIRECTORY}"
JOB_1_CAT_CMD="${HADOOP} fs -cat ${INTERMEDIATE_DIRECTORY1}/part* | wc -l"
JOB_2_CAT_CMD="${HADOOP} fs -cat ${INTERMEDIATE_DIRECTORY2}/part* | wc -l"
JOB_3_CAT_CMD="${HADOOP} fs -cat ${OUTPUT_DIRECTORY}/part* | wc -l"

# =========================================================
#
#  Start of the Main Body
#
# =========================================================
{
	# ---------------------------------------------------------
	# Clean-up the Environment before any MapReduce jobs.
	# ---------------------------------------------------------
	echo "+  ${CLEANUP_CMD}"
	${CLEANUP_CMD}

	# ---------------------------------------------------------
	# Perform the First MapReduce job.
	# ---------------------------------------------------------
	echo "+  ${JOB_1_CMD}"
	${JOB_1_CMD}
	if [ $? -ne 0 ]; then
		echo "ERROR OCCURRED DURING FIRST JOB. SEE ${LOG_FILE}"
#		${CLEANUP_CMD}
		exit $?
	else 
		echo "+  ${JOB_1_CAT_CMD}"
#		echo "FIRST JOB GENERATED `${JOB_1_CAT_CMD}` RECORDS"
	fi

	# ---------------------------------------------------------
	# Perform the Second MapReduce job.
	# ---------------------------------------------------------
	echo "+  ${JOB_2_CMD}"
	${JOB_2_CMD}
	if [ $? -ne 0 ]; then
		echo "ERROR OCCURRED DURING SECOND JOB. SEE ${LOG_FILE}"
#		${CLEANUP_CMD}
		exit $?
	else 
		echo "+  ${JOB_2_CAT_CMD}"
#		CAT_COUNT=`${JOB_2_CAT_CMD}`
		echo "SECOND JOB GENERATED ${CAT_COUNT} RECORDS"
	fi

	# ---------------------------------------------------------
	# Perform the Third MapReduce job.
	# ---------------------------------------------------------
	echo "+  ${JOB_3_CMD}"
	${JOB_3_CMD}
	if [ $? -ne 0 ]; then
		echo "ERROR OCCURRED DURING THIRD JOB. SEE ${LOG_FILE}"
#		${CLEANUP_CMD}
		exit $?
	else 
		echo "+  ${JOB_3_CAT_CMD}"
#		CAT_COUNT=`${JOB_3_CAT_CMD}`
		echo "THIRD JOB GENERATED ${CAT_COUNT} RECORDS"
	fi

	# ---------------------------------------------------------
	# Exit this script with no errors!
	# ---------------------------------------------------------
	echo "FINAL OUTPUT GENERATED AND STORED IN ${OUTPUT_DIRECTORY}"
	echo "SEE ${LOG_FILE} FOR MORE DETAILS ABOUT THE MAPREDUCE JOBS"
	exit 0


} & > ${LOG_FILE}
