#!/bin/bash
# =========================================================
# This script will run multiple MapReduce jobs with the 
# goal of generating data sets that can be easily unloaded 
# to produce the reports required for the NEISS data.
#
# This script depnd upon the NEISS data to already be 
# filtered and pre-processed before this script is executed.
#
# USAGE:  NEISS_Reports.sh
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
MAPREDUCE_JAR_JOB4="NEISSProductIncidents.jar"

# ---------------------------------------------------------
# Define the Driver Class Definitions for each Jar File.
# ---------------------------------------------------------
MAIN_CLASS_JOB1="edu.stthomas.gps.project.SimpleYearCount"
MAIN_CLASS_JOB2="edu.stthomas.gps.project.SimpleMonthCount"
MAIN_CLASS_JOB3="edu.stthomas.gps.project.SimpleAgeCount"
MAIN_CLASS_JOB4="edu.stthomas.gps.project.SimpleGenderCount"

# ---------------------------------------------------------
# Define the Directories needed for the multiple jobs.
# ---------------------------------------------------------
INPUT_DIRECTORY="${HDFS_PATH}/NEISS_KeywordFilter"
INTERMEDIATE_DIRECTORY="${HDFS_PATH}/NEISS_ScratchPad"
OUTPUT_DIRECTORY1="${HDFS_PATH}/NEISS_ReportByYear"
OUTPUT_DIRECTORY2="${HDFS_PATH}/NEISS_ReportByMonth"
OUTPUT_DIRECTORY3="${HDFS_PATH}/NEISS_ReportByAge"
OUTPUT_DIRECTORY4="${HDFS_PATH}/NEISS_ReportByGender"

# ---------------------------------------------------------
# Define the Execution Commands for each job.
# ---------------------------------------------------------
JOB_1_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB1} ${MAIN_CLASS_JOB1} ${INPUT_DIRECTORY} ${OUTPUT_DIRECTORY1} -totalIncidents 23828"
JOB_2_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB2} ${MAIN_CLASS_JOB2} ${INPUT_DIRECTORY} ${OUTPUT_DIRECTORY2} -totalIncidents 23828"
JOB_3_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB3} ${MAIN_CLASS_JOB3} ${INPUT_DIRECTORY} ${OUTPUT_DIRECTORY3} -totalIncidents 23828"
JOB_4_CMD="${HADOOP} jar ${LOCAL_PATH}/${MAPREDUCE_JAR_JOB4} ${MAIN_CLASS_JOB4} ${INPUT_DIRECTORY} ${OUTPUT_DIRECTORY4} -totalIncidents 23828"

CLEANUP_CMD="${HADOOP} fs -rm -r ${INTERMEDIATE_DIRECTORY} ${OUTPUT_DIRECTORY1} ${OUTPUT_DIRECTORY2} ${OUTPUT_DIRECTORY3} ${OUTPUT_DIRECTORY4}"
JOB_1_CAT_CMD="${HADOOP} fs -cat ${OUTPUT_DIRECTORY1}/part* | wc -l"
JOB_2_CAT_CMD="${HADOOP} fs -cat ${OUTPUT_DIRECTORY2}/part* | wc -l"
JOB_3_CAT_CMD="${HADOOP} fs -cat ${OUTPUT_DIRECTORY3}/part* | wc -l"
JOB_4_CAT_CMD="${HADOOP} fs -cat ${OUTPUT_DIRECTORY4}/part* | wc -l"

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
	# Perform the Fourth MapReduce job.
	# ---------------------------------------------------------
	echo "+  ${JOB_4_CMD}"
	${JOB_4_CMD}
	if [ $? -ne 0 ]; then
		echo "ERROR OCCURRED DURING FOURTH JOB. SEE ${LOG_FILE}"
#		${CLEANUP_CMD}
		exit $?
	else 
		echo "+  ${JOB_4_CAT_CMD}"
#		CAT_COUNT=`${JOB_4_CAT_CMD}`
		echo "FOURTH JOB GENERATED ${CAT_COUNT} RECORDS"
	fi

	# ---------------------------------------------------------
	# Exit this script with no errors!
	# ---------------------------------------------------------
	echo "FINAL OUTPUT GENERATED AND STORED IN ${OUTPUT_DIRECTORY}"
	echo "SEE ${LOG_FILE} FOR MORE DETAILS ABOUT THE MAPREDUCE JOBS"
	exit 0


} & > ${LOG_FILE}
