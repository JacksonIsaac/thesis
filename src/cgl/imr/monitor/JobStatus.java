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

package cgl.imr.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cgl.imr.base.impl.JobConf;
import cgl.imr.message.TaskStatus;

/**
 * Presents the state of the job to the user. Keeps track of successful/failed
 * tasks.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class JobStatus {

	private JobConf jobConf;
	private Map<Integer, TaskStatus> mapTaskStatuses;
	private int numCombineInputsReceived = 0;

	private int numFailedMapTasks = 0;
	private int numFailedReduceTasks = 0;

	private int numSuccessfulMapTasks = 0;
	private int numSuccessfulReduceTasks = 0;
	
	private boolean success=true;;
		
	private Map<Integer, TaskStatus> reduceTaskStatuses;

	public JobStatus(JobConf jobConf) {
		this.jobConf = jobConf;
		this.mapTaskStatuses = new ConcurrentHashMap<Integer, TaskStatus>();
		this.reduceTaskStatuses = new ConcurrentHashMap<Integer, TaskStatus>();
	}
	
	public boolean isSuccess(){
		return success;
	} 
	
	public void setFailed(){
		success=false;
	}

	public void addMapTaskStatus(TaskStatus mapStatus) {
		this.mapTaskStatuses.put(mapStatus.getTaskNo(), mapStatus);
	}

	public void addReduceTaskStatus(TaskStatus reduceStatus) {
		this.reduceTaskStatuses.put(reduceStatus.getTaskNo(), reduceStatus);
	}

	public float getCurrentMapPercentage() {
		return ((float) numSuccessfulMapTasks) / jobConf.getNumMapTasks() * 100;
	}

	public float getCurrentReducePercentage() {
		return ((float) numSuccessfulReduceTasks) / jobConf.getNumReduceTasks()
				* 100;
	}

	public Map<Integer, TaskStatus> getMapTaskStatuses() {
		return mapTaskStatuses;
	}

	public TaskStatus getMapTaskStatuses(int mapTaskNumber) {
		return mapTaskStatuses.get(new Integer(mapTaskNumber));
	}

	public int getNumCombineInputsReceived() {
		return numCombineInputsReceived;
	}

	public int getNumFailedMapTasks() {
		return numFailedMapTasks;
	}

	public int getNumFailedReduceTasks() {
		return numFailedReduceTasks;
	}

	public int getNumSuccessfulMapTasks() {
		return numSuccessfulMapTasks;
	}

	public int getNumSuccessfulReduceTasks() {
		return numSuccessfulReduceTasks;
	}

	public Map<Integer, TaskStatus> getReduceTaskStatuses() {
		return reduceTaskStatuses;
	}

	public TaskStatus getReduceTaskStatuses(int reduceTaskNumber) {
		return reduceTaskStatuses.get(new Integer(reduceTaskNumber));
	}

	public void incrementCombineInputs() {
		numCombineInputsReceived++;
	}

	public void incrementFailedMapTasks() {
		numFailedMapTasks++;
	}

	public void incrementFailedReduceTasks() {
		numFailedReduceTasks++;
	}

	public void incrementSuccessfulMapTasks() {
		numSuccessfulMapTasks++;
	}

	public void incrementSuccessfulReduceTasks() {
		numSuccessfulReduceTasks++;
	}
}
