/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.base;

import cgl.imr.types.StringKey;

/**
 * Common place for the constants associated with the framework.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public interface TwisterConstants {

	// Job States
	public enum JobState {
		COMBINE_COMPLETE, COMBINE_STARTED, INITIATED, MAP_COMPLETE, MAP_CONFIGURED, MAP_CONFIGURING, MAP_SUBMITTED, NOT_CONFIGURED, REDUCE_COMPLETE, REDUCE_CONFIGURED, REDUCE_CONFIGURING, TERMINATE_COMPLETES;
	}

	public enum EntityType {
		DRIVER, DAEMON;
	}
	
	public enum SendRecvStatus{
		SUCCESS,EXCEPTION,FALIURE;
	}
	
	public enum DriverMethodStatus{
		SUCCESS,EXCEPTION,FAILURE;
	}

	byte COMBINE_INPUT = 11;
	String COMBINE_TOPIC_BASE = "/twister/combine/topic";
	String MAP_REDUCE_TOPIC_BASE = "/twister/map-reduce/submit/topic";
	String CLEINT_TO_WORKER_BCAST = "/twister/client/to/worker/bcast/topic";
	String MAP_TO_REDUCE_ROW_WISE_BCAST = "/twister/map/to/reduce/bcast/topic";
	byte DIR_LIST_REQ = 13;
	byte DIR_LIST_RES = 14;

	byte FAILED = 1;
	// Configurations
	String FIXED_DATA_FILE = "fixed_data";
	String FIXED_DATA_MODEL = "fixed_data_model";
	String HEP_DATA_STRING = "hep_data_string";
	byte MAP_ITERATIONS_OVER = 9;

	// Tasks
	byte MAP_TASK = 0;
	byte MAP_TASK_REQUEST = 3;
	byte MAP_WORKER_STARTED = 10;
	// Message types
	byte MAPPER_REQUEST = 1;
	byte MONITOR_REQUEST = 7;
	byte MONITOR_RESPONSE = 8;
	byte MEMCACHE_INPUT = 18;
	byte MEMCACHE_CLEAN = 19;
	byte DAEMON_STATUS = 20;
	byte START_REDUCE = 21;
	// Timings
	long MONITOR_SLEEP_TIME = 5; // milliseconds.
	byte NEW_JOB_REQUEST = 16;
	byte NEW_JOB_RESPONSE = 17;
	String NUM_MAP_TASKS = "num_map_tasks";
	String NUM_REDUCE_TASKS = "num_reduce_tasks";

	// Topics
	String PARTITION_FILE_RESPONSE_TOPIC_BASE = "/dir/list/response/topic/";
	String PARTITION_FILE_SPLIT_PATTERN = ",";

	String PROPERTIES_FILE = "twister.properties";
	byte REDUCE_INPUT = 5;

	byte REDUCE_RESPONSE = 6;

	byte REDUCE_TASK = 1;
	byte REDUCE_TASK_REQUEST = 4;
	String REDUCE_TOPIC_BASE = "/twister/reduce/topic";
	byte REDUCE_WORKER_REQUEST = 2;
	String RESPONSE_TOPIC_BASE = "/twister/response/for/client/topic";
	
	
	// ZBJ: normal send and receive waiting
	long SEND_RECV_MAX_SLEEP_TIME = 180000; // milliseconds
	
	//ZBJ: this is for creating partition file
	long SEND_RECV_POLLNODE_MAX_SLEEP_TIME = 12000;
	
	/*
	 * ZBJ: 
	 * The reason why changing this from 600000 is that originally long waiting
	 * time could be wrongly assumed as a fault by FaultDetector This is also
	 * fixed through updating the the status at the end of sending new job. See
	 * TwisterDriver initialization about sending new jobs.
	 */
	long SEND_RECV_NEWJOB_MAX_SLEEP_TIME = 12000;
	// ZBJ: loading file could take long time
	long SEND_RECV_MAPREQUEST_MAX_SLEEP_TIME = 600000;
	long SEND_RECV_SLEEP_TIME = 2; // milliseconds
	
	long MAX_WAIT_TIME_FOR_FAULT = 30000;

	byte SUCCESS = 0;

	byte TASK_STATUS = 12;
	byte WORKER_RESPONSE = 15;
	byte REDUCE_INPUT_URI=16;
	
	
	int NUM_RETRIES =3;
	int WAIT_COUNT_FOR_FAULTS=5000;
	//int WAIT_BEFORE_RETRY_RECOVERY=15000;

    StringKey fixed_key_M2R = new StringKey("fixed_key_M2R_4a616c697961");

    int indirect_transfer_threashold = 10*1024*1024; // 10MB
	
	StringKey fixed_key_R2C = new StringKey("fixed_key_R2C_4a616c697961");
	
}
