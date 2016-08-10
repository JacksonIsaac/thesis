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

import org.apache.log4j.Logger;

import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.impl.JobConf;
import cgl.imr.message.TaskStatus;

/**
 * Monitor the progress of MapReduce computation tasks.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class TwisterMonitor implements TwisterConstants {

	private static Logger logger = Logger.getLogger(TwisterMonitor.class);

	private TwisterModel driver;
	private boolean hasCombiner = false;
	private boolean hasMonitoringException = false;
	private JobConf jobConf;
	private JobStatus jobStatus;

	private Exception monitoringException;

	public TwisterMonitor(JobConf jobConf, TwisterModel driver) {
		this.driver = driver;
		this.jobConf = jobConf;
		this.jobStatus = new JobStatus(jobConf);
	}

	public void combinerInputReceived() {
		this.jobStatus.incrementCombineInputs();
	}

	private int getElapsedTimeInMinutes(long beginTime) {
		return (int) ((System.currentTimeMillis() - beginTime) / (1000 * 60));
	}

	public Exception getMonitoringException() {
		return monitoringException;
	}

	/**
	 * Check if the monitoring is complete.
	 * 
	 * @return true if completes, otherwise false.
	 * @throws TwisterException
	 */
	public boolean isComplete() throws TwisterException {
		if (hasMonitoringException) {
			throw new TwisterException(monitoringException);
		}

		if (jobStatus.getNumSuccessfulMapTasks() == jobConf.getNumMapTasks()
				&& jobStatus.getNumSuccessfulReduceTasks() == jobConf
						.getNumReduceTasks()) {
			if (hasCombiner) {
				if (this.jobStatus.getNumCombineInputsReceived() == jobConf
						.getNumReduceTasks()) {
					return true;
				} else {
					return false;
				}
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

	public boolean isHasCombiner() {
		return hasCombiner;
	}

	/**
	 * Monitors till the completion of the job.
	 * 
	 * @return job status.
	 * @throws TwisterException
	 */
	public JobStatus monitorTillCompletion() throws TwisterException {

		while (!isComplete()) {
			try {
				Thread.sleep(TwisterConstants.MONITOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				e.printStackTrace();// TODO logit.
			}
			if (hasMonitoringException) {
				throw new TwisterException(monitoringException);
			}
		}
		this.driver.setMonitoringCompletes();
		return this.jobStatus;
	}

	/**
	 * Wait till the completion of the job or till the given number of minutes.
	 * 
	 * @param maxMinutes
	 *            - Maximum number of minutes to wait.
	 * @return job status
	 * @throws TwisterException
	 */
	public JobStatus monitorTillCompletion(int maxMinutes)
			throws TwisterException {
		long beginTime = System.currentTimeMillis();
		while (!(isComplete() || getElapsedTimeInMinutes(beginTime) > maxMinutes)) {
			try {
				Thread.sleep(TwisterConstants.MONITOR_SLEEP_TIME);
			} catch (InterruptedException e) {
				logger.error(e);
			}
			if (hasMonitoringException) {
				throw new TwisterException(monitoringException);
			}
		}
		this.driver.setMonitoringCompletes();
		return this.jobStatus;
	}

	/**
	 * This method receives all the monitoring related events and handles them
	 * appropriately.
	 */
	public void onEvent(byte[] message) {
		if (message != null) {
			if (message[0] == TwisterConstants.TASK_STATUS) {
				try {
					TaskStatus status = new TaskStatus(message);
					if (status.getTaskType() == MAP_TASK) {
						if (status.getStatus() == SUCCESS) {
							this.jobStatus.incrementSuccessfulMapTasks();
						} else if (status.getStatus() == FAILED) {
							this.jobStatus.incrementFailedMapTasks();
						}
						this.jobStatus.addMapTaskStatus(status);
					} else if (status.getTaskType() == REDUCE_TASK) {
						if (status.getStatus() == SUCCESS) {
							this.jobStatus.incrementSuccessfulReduceTasks();
						} else if (status.getStatus() == FAILED) {
							this.jobStatus.incrementFailedReduceTasks();
						}
						this.jobStatus.addReduceTaskStatus(status);
					}
				} catch (Exception e) {
					setMonitoringException(new Exception(
							"Monitor encoutered errors.", e));
				}
			}
		}
	}

	public void resetMonitor() {
		this.hasMonitoringException = false;
		this.monitoringException = null;
		this.jobStatus = new JobStatus(jobConf);
	}

	public void setHasCombiner(boolean hasCombiner) {
		this.hasCombiner = hasCombiner;
	}

	public void setMonitoringException(Exception monitoringException) {
		this.hasMonitoringException = true;
		this.monitoringException = monitoringException;
	}
}
