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

package cgl.imr.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.KeyValuePair;
import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.data.DataPartitionException;
import cgl.imr.data.file.FileData;
import cgl.imr.data.file.FileDataPartitioner;
import cgl.imr.data.file.PartitionFile;
import cgl.imr.message.CombineInput;
import cgl.imr.message.EndJobRequest;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.MemCacheClean;
import cgl.imr.message.MemCacheInput;
import cgl.imr.message.NewJobRequest;
import cgl.imr.message.PubSubMessage;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.StartReduceMessage;
import cgl.imr.message.WorkerResponse;
import cgl.imr.monitor.TwisterMonitorBasic;
import cgl.imr.types.IntValue;
import cgl.imr.types.StringKey;
import cgl.imr.types.StringValue;

/**
 * Client side driver for the MapReduce computations. This is a very important
 * class in Twister framework. Many extensions possible to add features such as
 * fault tolerance etc..
 *
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 *
 */
public class TwisterDriver implements Subscribable, TwisterConstants,
		TwisterModel {

	private static Logger logger = Logger.getLogger(TwisterDriver.class);

	protected String combineTopic;
	protected Combiner currentCombiner;
	protected boolean isMonitoringMapReduce = false;
	protected JobConf jobConf;
	protected JobState jobState;
	protected boolean mapConfigured = false;

	protected TwisterMonitor monitor;
	protected TwisterConfigurations mrConfig;
	protected int numMapTasks;
	// protected int numDaemons;
	protected int numDaemonsPerNode;
	protected int numReduceTasks;
	protected PubSubService pubSubService;
	protected Random randomizer;
	protected boolean reduceConfigured = false;
	protected String reduceTopicBase;
	protected String responseTopic;
	protected ShutdownHook shutDownHook;

	protected ConcurrentHashMap<Integer, TaskAssignment> reduceTasksMap = new ConcurrentHashMap<Integer, TaskAssignment>();
	protected ConcurrentHashMap<Integer, TaskAssignment> mapTasksMap = new ConcurrentHashMap<Integer, TaskAssignment>();
	protected ConcurrentHashMap<String, WorkerResponse> responseMap = new ConcurrentHashMap<String, WorkerResponse>();
	protected ConcurrentHashMap<Integer, TaskAssignment> memCacheMap = new ConcurrentHashMap<Integer, TaskAssignment>();
	protected UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	protected int iterationCount = 0;

	protected FaultDetector faultDetector;
	protected ExecutionPlan execPlan;

	protected List<Integer> workingDaemons;

	// ZBJ: added for automizing the failure handling
	private List<KeyValuePair> lastKeyValuePair;
	private Value lastBcastValue;

	public TwisterDriver() {
		this.jobState = JobState.INITIATED;
		this.numMapTasks = 384;
		this.setLastBcastValue(null);
		this.setLastKeyValuePair(null);
	}

	/**
	 * Constructor for the TwisterDriver. Takes the JobConf as the input and
	 * establishes a connection with the broker network. Then it proceeds to
	 * subscribe into the necessary topics and initialize various data
	 * structures necessary.
	 *
	 * @param jobConf
	 *            - JobConf object relevant to this job.
	 * @throws TwisterException
	 */
	public TwisterDriver(JobConf jobConf) throws TwisterException {
		this.setLastBcastValue(null);
		this.setLastKeyValuePair(null);

		this.jobState = JobState.NOT_CONFIGURED;
		if (jobConf == null) {
			throw new TwisterException("JobConfiguration cannot be null.");
		}
		System.out.println("JobID: " + jobConf.getJobId());
		this.jobConf = jobConf;
		this.numMapTasks = jobConf.getNumMapTasks();
		this.numReduceTasks = jobConf.getNumReduceTasks();

		workingDaemons = new ArrayList<Integer>();

		// System.out.println("Configuring Twister");

		try {
			this.mrConfig = TwisterConfigurations.getInstance();
			int numDaemons = getNumDaemonsFromConfigFiles();
			for (int i = 0; i < numDaemons; i++) {
				workingDaemons.add(i);
			}

			this.numDaemonsPerNode = mrConfig.getDamonsPerNode();
		} catch (ConfigurationException e) {
			throw new TwisterException(
					"Could not load the Twister configurations. Please check if "
							+ TwisterConstants.PROPERTIES_FILE
							+ " exists in the classpath.", e);
		} catch (IOException e) {
			throw new TwisterException(
					"Could not read the nodes file. Please check if  the nodes file "
							+ "exists in the classpath.", e);
		}
		this.responseTopic = TwisterConstants.RESPONSE_TOPIC_BASE + "/"
				+ jobConf.getJobId();
		this.reduceTopicBase = TwisterConstants.REDUCE_TOPIC_BASE + "/"
				+ uuidGen.generateTimeBasedUUID();
		this.combineTopic = TwisterConstants.COMBINE_TOPIC_BASE
				+ uuidGen.generateTimeBasedUUID();

		// System.out.println("Adding a shutdown hook");

		// Adds a shutdown hook.
		shutDownHook = new ShutdownHook(this);
		// System.out.println("Created a shutdown hook");
		Runtime.getRuntime().addShutdownHook(new Thread(shutDownHook));
		// System.out.println("Adding the hook to runtime");
		try {
			int entityId = new Random(System.currentTimeMillis()).nextInt() * 1000000; // entity
			// ids for drivers starts from 100000 and above.
			this.pubSubService = PubSubFactory.getPubSubService(mrConfig,
					EntityType.DRIVER, entityId);
			// System.out.println("Setting the subscriber");
			this.pubSubService.setSubscriber(this);
			this.pubSubService.subscribe(responseTopic);
			this.pubSubService.subscribe(combineTopic);
			// System.out.println("Subscribed to pubSubService");

		} catch (PubSubException e) {
			if (this.pubSubService != null) {
				try {
					this.pubSubService.close();
				} catch (PubSubException e1) { // Ignore
				}
			}
			throw new TwisterException(
					"Could not establish a connection with the pub/sub broker.",
					e);
		}

		// System.out.println("Starting a Randomizer");
		randomizer = new Random(System.currentTimeMillis());
		this.monitor = new TwisterMonitorBasic(jobConf, this);

		try {
			/*
			 * ZBJ: Once there were faults in daemons, sendNewJobRequest may not
			 * work. It will take long time and doesn't give any response to the
			 * user.
			 */
			//System.out.println("Start sending new job request");
			Map<Integer, DaemonStatus> daemons = sendNewJobRequest(
					jobConf.getJobId(), workingDaemons);

			/*
			System.out.println("Daemons " + daemons.size() + " "
					+ daemons.get(0).isRunning() + " "
					+ daemons.get(1).isRunning());
			*/

			/*
			 * fault detector starts here, if I waited for some daemons in  sendNewJobRequest long time
			 * It will cause long interval between two daemon status, fault detector will consider it
			 * as a fault
			 */
			faultDetector = new FaultDetector(daemons,
					jobConf.isFaultTolerance());

			//updateWorkingDaemons(daemons.keySet());
			/*
			 * change code to get available daemons which can work, FaultDetector may not be active
			 * according to user's settings, but getAvailableDeamons() can still be used.
			 */
			updateWorkingDaemons(faultDetector.getAvailableDeamons());
			execPlan = new ExecutionPlan();
		} catch (Exception e) {
			throw new TwisterException(e);
		}

		this.jobState = JobState.INITIATED;

		// System.out.println("Job Initiated");
	}

	private synchronized void updateWorkingDaemons(Collection<Integer> daemons) {
		this.workingDaemons.clear();
		this.workingDaemons.addAll(daemons);
	}

	public FaultDetector getFaultDetector() {
		return faultDetector;
	}

	protected void cleanupAndTerminateJob() {
		try {
			if (this.jobState.ordinal() < JobState.INITIATED.ordinal()) {
				logger.error("TwisterDriver failed. Cleaning up.");
			} else if (this.jobState.ordinal() == JobState.INITIATED.ordinal()) {
				logger.error("TwisterDriver failed. Removing the broker connection.");
				this.pubSubService.close();
				this.faultDetector.close();
			} else if (this.jobState.ordinal() < JobState.TERMINATE_COMPLETES
					.ordinal()) {
				terminate();
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#close()
	 */
	public void close() {
		if (this.jobState.ordinal() != JobState.TERMINATE_COMPLETES.ordinal()) {
			cleanupAndTerminateJob();
		}
	}

	/**
	 * Configure the current combiner to use witht this MapReduce comptuation.
	 *
	 * @throws TwisterException
	 */
	protected void configureCurrentCombiner() throws TwisterException {
		try {
			Class<?> combinerClass = Class.forName(jobConf.getCombinerClass());
			this.currentCombiner = (Combiner) combinerClass.newInstance();
			this.currentCombiner.configure(jobConf);
		} catch (Exception e) {
			throw new TwisterException("Could not load combiner class.", e);
		}
	}

	private boolean configureMapsInternal() throws TwisterException {
		if (jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
			cleanupAndTerminateJob();
			throw new TwisterException("Maps can be configured only once.");
		}
		jobState = JobState.MAP_CONFIGURING;
		List<Integer> availableDaemons = faultDetector.getAvailableDeamons();
		int numAvailableDaemons = availableDaemons.size();

		// ZBJ: do some output here, test which daemons are available
		/*
		System.out.print("Available daemons: ");
		for (int i = 0; i < availableDaemons.size(); i++) {
			System.out.print(availableDaemons.get(i) + " ");
		}
		System.out.println();
		*/

		// ZBJ: stop computing if no daemons available
		if (numAvailableDaemons == 0) {
			throw new TwisterException("No Available Daemons.");
		}

		MapperConf mapperConf = null;
		TaskAssignment mapAssignment = null;
		MapperRequest mapperRequest = null;
		for (int m = 0; m < numMapTasks; m++) {
			mapperConf = new MapperConf(m);
			mapperRequest = new MapperRequest(jobConf, mapperConf,
					iterationCount);
			mapperRequest.setResponseTopic(responseTopic);
			/*
			 * When the maps don't have any static data we don't need to worry
			 * about where the maps going to run. Just load balance it.
			 */
			mapAssignment = new TaskAssignment(mapperRequest,
					availableDaemons.get(m % numAvailableDaemons));
			mapTasksMap.put(new Integer(m), mapAssignment);
		}
		SendRecvResponse sendRecvResponse = null;
		// Map<Integer, WorkerResponse> responses = null;
		// WorkerResponse response = null;
		try {
			sendRecvResponse = sendAllExecutorRequestsAndReceiveResponses(
					mapTasksMap, numMapTasks);
		} catch (Exception e) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Could not send the map executor requests.", e);
		}
		if (sendRecvResponse != null
				&& sendRecvResponse.getStatus()
						.equals(SendRecvStatus.EXCEPTION)) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"ConfigureMaps produced errors at the daemons. Please see the logs for further information.");
			// responses = sendRecvResponse.getWorkerReponses();
			// Iterator<Integer> resIte = responses.keySet().iterator();
			// while (resIte.hasNext()) {
			// response = responses.get(resIte.next());
			// if (response.isHasException()) {
			// throw new TwisterException(
			// "Could not send all the map executor requests. First error is "
			// + response.getExceptionString());
			// }
			// }
		} else if (sendRecvResponse != null
				&& sendRecvResponse.getStatus().equals(SendRecvStatus.SUCCESS)) {
			this.mapConfigured = true;
			jobState = JobState.MAP_CONFIGURED;
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#configureMaps()
	 */
	public void configureMaps() throws TwisterException {
		// configureMapsInternal();

		/*
		 * ZBJ: original code only invoke configureMapsInternal (see above) new
		 * code added here, to make the invoke fault tolerable.
		 * execPlan.setMapConfigured() is to tell execPlan is configured.
		 * Original execPlan only handle the case that map has partition file
		 * and values
		 */
		execPlan.setMapConfigured();
		boolean status = configureMapsInternal();
		if (!status) {
			if (jobConf.isFaultTolerance()) {
				System.out.println("Begin to handle Failure in configureMaps()");
				handleFailures();
			} else {
				throw new TwisterException("ConfigureMaps failed.");
			}
		}

		//
		// int numRetries=1;
		// int retryCount=0;
		// if(jobConf.isFaultTolerance()){
		// numRetries=NUM_RETRIES;
		// }
		// while(retryCount<numRetries){
		//
		// }
		// if(retryCount<NUM_RETRIES){
		// this.mapConfigured = true;
		// jobState = JobState.MAP_CONFIGURED;
		// }else{
		// throw new
		// TwisterException("Maximum recovery attempts were made. Could not recover from the failure.");
		// }
	}

	/**
	 * Configuring map tasks. This is to allow users to configure/load any
	 * static data that can be used in the map tasks. Takes a
	 * <code>DataPartitioner</code> object as input and sends partition
	 * information to all the worker nodes appropriately.
	 *
	 * @param partitioner
	 *            - A data partitioner containing information on about data
	 *            distribution.
	 * @throws TwisterException
	 */
	// protected void configureMaps(DataPartitioner partitioner)
	// throws TwisterException {
	// if (jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
	// cleanupAndTerminateJob();
	// throw new TwisterException("Maps can be configured only once.");
	// }
	// jobState = JobState.MAP_CONFIGURING;
	// MapperConf mapperConf = null;
	// TaskAssignment mapAssignment = null;
	// MapperRequest mapperRequest = null;
	// try {
	// for (int m = 0; m < numMapTasks; m++) {
	// mapperConf = new MapperConf(m, partitioner.getPartition(m));
	// mapperRequest = new MapperRequest(jobConf, mapperConf,
	// iterationCount);
	// mapperRequest.setResponseTopic(responseTopic);
	//
	// // FIX THIS.
	// mapAssignment = new
	// TaskAssignment(mapperRequest,getAssignedDaemonForMap(partitioner.getAssignedNode(m)));
	//
	//
	// mapTasksMap.put(new Integer(m), mapAssignment);
	// }
	// } catch (DataPartitionException e) {
	// throw new TwisterException(e);
	// }
	// Map<Integer, WorkerResponse> errors = null;
	// try {
	// errors = sendAllExecutorRequestsAndReceiveResponses(mapTasksMap,
	// numMapTasks);
	// } catch (Exception e) {
	// cleanupAndTerminateJob();
	// throw new TwisterException(
	// "Could not send the map executor requests.", e);
	// }
	// if (errors != null && !errors.isEmpty()) {
	// // TODO : Re-try. Improvements for fault tolerance.
	// cleanupAndTerminateJob();
	// throw new TwisterException(
	// "Could not send all the map executor requests. First error is "
	// + errors.values().iterator().next()
	// .getExceptionString());
	// }
	// this.mapConfigured = true;
	// jobState = JobState.MAP_CONFIGURED;
	// }

	// protected Map<Integer,List<Integer>> getAvailableNodesAndDaemons(){
	//
	// }

	public boolean configureMapsInternal(String partitionFile)
			throws TwisterException {
		logger.info("Configure Mappers through the partition file, please wait....");
		if (jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
			cleanupAndTerminateJob();
			throw new TwisterException("Maps can be configured only once.");
		}

		try {
			List<Integer> availableDaemons = faultDetector
					.getAvailableDeamons();

			/*
			 * ZBJ: do some output here, to test which daemons are still
			 * available. however, it looks Twister assumes that all node are
			 * available at starting, we need to apply this strategy to the
			 * start of Twister in future.
			 */

			/*
			 * System.out.print("Available daemons: "); for (int i = 0; i <
			 * availableDaemons.size(); i++) {
			 * System.out.print(availableDaemons.get(i) + " "); }
			 * System.out.println();
			 */


			PartitionFile partitions = new PartitionFile(partitionFile);

			if (numMapTasks != partitions.getNumberOfFiles()) {
				throw new DataPartitionException(
						"Number of maps should be equal to the number of data partions.");
			}

			Map<String, Integer> partitionsAndDaemons = FileDataPartitioner
					.assignPartitionsToDaemons(availableDaemons, partitions);

			jobState = JobState.MAP_CONFIGURING;
			MapperConf mapperConf = null;
			TaskAssignment mapAssignment = null;
			MapperRequest mapperRequest = null;
			int count = 0;
			int assignedDaemon;
			String dataFile;
			FileData fileData;

			Iterator<String> ite = partitionsAndDaemons.keySet().iterator();
			while (ite.hasNext()) {
				dataFile = ite.next();
				assignedDaemon = partitionsAndDaemons.get(dataFile);
				fileData = new FileData(dataFile);
				mapperConf = new MapperConf(count, fileData);
				mapperRequest = new MapperRequest(jobConf, mapperConf,
						iterationCount);
				mapperRequest.setResponseTopic(responseTopic);
				mapAssignment = new TaskAssignment(mapperRequest,
						assignedDaemon);
				mapTasksMap.put(new Integer(count), mapAssignment);
				count++;
			}
			// System.out.println("can internal map configuration arrive here ?");
			/*
			 * Sometimes, this part of code takes long time to execute. Sending
			 * could fail with unknown reasons.
			 */
			SendRecvResponse sendRecvResponse = null;
			try {
				// System.out.println("Begin to send");
				sendRecvResponse = sendAllExecutorRequestsAndReceiveResponses(
						mapTasksMap, numMapTasks);
				// System.out.println("Will I send successfully?");
			} catch (Exception e) {
				cleanupAndTerminateJob();
				throw new TwisterException(
						"Could not send the map executor requests.", e);
			}
			// System.out.println("The way toward the truth!");
			if (sendRecvResponse != null
					&& sendRecvResponse.getStatus().equals(
							SendRecvStatus.EXCEPTION)) {
				cleanupAndTerminateJob();
				throw new TwisterException(
						"ConfigureMaps produced errors at the daemons. Please see the logs for further information.");
				// responses = sendRecvResponse.getWorkerReponses();
				// Iterator<Integer> resIte = responses.keySet().iterator();
				// while (resIte.hasNext()) {
				// response = responses.get(resIte.next());
				// if (response.isHasException()) {
				// throw new TwisterException(
				// "ConfigureMaps produced errors. First error is "+
				// response.getExceptionString());
				// }
				// }

			} else if (sendRecvResponse != null
					&& sendRecvResponse.getStatus().equals(
							SendRecvStatus.SUCCESS)) {
				// System.out.println("should be true if I am here");
				this.mapConfigured = true;
				jobState = JobState.MAP_CONFIGURED;
				logger.info("Configuring Mappers through the partition file is completed. ");
				return true;
			} else {
				/*
				 * Sometimes, code comes here since the status is Failed.
				 */
				/*
				 * System.out.println("No sendRecvResponse ?"); if
				 * (sendRecvResponse != null) { System.out.println("status: " +
				 * sendRecvResponse.getStatus()); }
				 */
			}

		} catch (DataPartitionException e) {
			// System.out.println("Exception happens?");
			throw new TwisterException(e);
		}
		// System.out.println("finally wrong");
		return false;
	}

	public void configureMaps(String partitionFile) throws TwisterException {
		execPlan.setPartitionFile(partitionFile);
		boolean status = configureMapsInternal(partitionFile);
		if (!status) {
			if (jobConf.isFaultTolerance()) {
				System.out.println("Begin to handle Failure in configureMaps(String partitionFile). ");
				handleFailures();
			} else {
				throw new TwisterException("ConfigureMaps failed.");
			}
		}
	}


	/**
	 * ZBJ: Configure Maps by the data partitions among the distributed data directories.
	 *
	 * This function is a combination of create partition file and configureMaps through partition file
	 *
	 * @param dir
	 * @param file_filter
	 * @throws TwisterException
	 */
	public void configureMaps(String dir, String file_filter) throws TwisterException {
		PartitionFileCreator pc;
		try {
			pc = new PartitionFileCreator();
			pc.pollNodesAndCreateParitionFile(dir, file_filter, "tmp.pf");
			pc.close();
			logger.info("Partition File Created.");
		} catch (Exception e) {
			logger.error("PartitionFileCreate failed.", e);
		}

		configureMaps("tmp.pf");
	}

	public boolean handleFailures() throws TwisterException {
		logger.info("Handling the failure.");
		// ZBJ: stop reporting failure here,
		// let fault handler to check by itself
		this.faultDetector.setFaultHandlerIsWorking(true);

		double beginTime = System.currentTimeMillis();
		int numRetries = 0;
		boolean complete = true;
		while (numRetries < NUM_RETRIES) {
			// Clean up the crashed job
			logger.info("Recovering, TRY " + numRetries +"!");
			logger.info("Clean up after a fault, Waiting...");
			// ZBJ: move update daemon in front of cleanupAfterAFault in order not to send
			// message to failed daemons, currently not sure of the effect
			updateWorkingDaemons(faultDetector.getAvailableDeamons());
			cleanupAfterAFault();
			//updateWorkingDaemons(faultDetector.getAvailableDeamons());
			logger.info("Cleanup Completed.");

			// Send a new job Request.
			logger.info("Send New Job.");
			try {
				synchronized (this) {
					Map<Integer, DaemonStatus> daemons = sendNewJobRequest(
							jobConf.getJobId(), workingDaemons);
					this.faultDetector.updateDaemonStatus(daemons);
					updateWorkingDaemons(daemons.keySet());
				}
			} catch (Exception e) {
				throw new TwisterException(e);
			}
			this.jobState = JobState.INITIATED;
			logger.info("Sending New Job Completed.");

			if (execPlan.getNumOnlineMemCaches() > 0) {
				ConcurrentHashMap<String, Value> cachedData = execPlan
						.getMemCachedData();
				Iterator<String> keys = cachedData.keySet().iterator();
				String key;
				while (keys.hasNext()) {
					key = keys.next();
					complete = addToMemCacheInternal(key, cachedData.get(key));
					if (!complete) {
						continue;
					}
				}
				if (!complete) {
					numRetries++;
					continue;
				}
			}
			logger.info("Mem cache restored.");
			logger.info("Configure Maps. ");
			if (execPlan.isMapConfigured()) {
				// System.out.println("execPlan has map partition file");
				if (execPlan.getPartitionFile() != null) {
					complete = configureMapsInternal(execPlan
							.getPartitionFile());
				} else if (execPlan.getMapConfigurations() != null) {
					// System.out.println("map is configured by values");
					complete = configureMapsInternal(execPlan
							.getMapConfigurations());
				} else {
					// ZBJ: use configureMaps() without parameter will go here
					complete = configureMapsInternal();
				}
				if (!complete) {
					System.out.println("Map reconfiguration can not be completed, retry...");
					numRetries++;
					continue;
				}
			}

			logger.info("Maps Configured.");
			if (execPlan.isReduceConfigured()) {
				complete = configureReduceInternal(execPlan
						.getReduceConfigurations());
				if (!complete) {
					numRetries++;
					continue;
				}
			} else {
				complete = configureReduceInternal(null);
				if (!complete) {
					numRetries++;
					continue;
				}
			}
			logger.info("Reduce tasks configured.");

			if (complete) {
				break;
			}

			/*
			 * if complete, we don't need to numRetries++ to try one more time.
			 * So add numRetries at the end.
			 */
			numRetries++;

			// try {
			// Thread.sleep(WAIT_BEFORE_RETRY_RECOVERY);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }//Wait fe
		}
		// ZBJ: make sure if rerun mapreduce is required,
		if (numRetries < NUM_RETRIES && isMonitoringMapReduce) {
			rerunMapReduce();
		}
		faultDetector.setLastFaultServicedTime(System.currentTimeMillis());
		this.faultDetector.setFaultHandlerIsWorking(false);

		double endTime = System.currentTimeMillis();
		logger.info("Total Time to recover from the failure ="
				+ (endTime - beginTime) / 1000 + " Seconds.");
		if (numRetries < NUM_RETRIES) {
			return true;
		}
		return false;
	}

	protected void cleanupAfterAFault() {
		EndJobRequest endMapReduceRequest = new EndJobRequest();
		endMapReduceRequest.setJobId(jobConf.getJobId());
		endMapReduceRequest.setResponseTopic(responseTopic);
		try {
			bcastRequestsAndReceiveResponses(endMapReduceRequest);
		} catch (Exception e) {
			logger.error("Could not send the teminate requests.", e);
		}

		// Now clean the maps.
		reduceTasksMap.clear();
		mapTasksMap.clear();
		responseMap.clear();
		memCacheMap.clear();
	}

	public static void main(String[] args) throws TwisterException {
		TwisterDriver driver = new TwisterDriver();
		driver.configureMaps("D:/projects/phdprj/javaworkspace/Twister/pat.pf");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#cleanMemCache(java.lang.String)
	 */
	public void cleanMemCache(String key) throws TwisterException {
		MemCacheClean cacheClean = new MemCacheClean(jobConf.getJobId(), key);
		try {
			pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
					cacheClean.getBytes());
			execPlan.removeMemCachedData(key);
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#addToMemCache(cgl.imr.base.Value)
	 */
	public String addToMemCache(Value value) throws TwisterException {
		String key = uuidGen.generateRandomBasedUUID().toString();
		execPlan.addMemCachedData(key, value);
		boolean status;
		status = addToMemCacheInternal(key, value);

		if (!status) {
			if (jobConf.isFaultTolerance()) {
				System.out
						.println("Begin to handle Failure in addToMemCache(Value value)");
				handleFailures();
			} else {
				throw new TwisterException("AddToMemCache failed.");

			}
		}
		return key;
	}

	protected boolean addToMemCacheInternal(String key, Value value)
			throws TwisterException {
		MemCacheInput cacheInput = new MemCacheInput(jobConf.getJobId(), key,
				value);
		cacheInput.setResponseTopic(responseTopic);

		SendRecvResponse response;
		try {
			response = bcastRequestsAndReceiveResponses(cacheInput);
		} catch (Exception e) {
			throw new TwisterException(e);
		}
		if (response != null
				&& response.getStatus().equals(SendRecvStatus.EXCEPTION)) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Add to mem cache produced errors at the daemons. Please see the logs for further information.");
		} else if (response != null
				&& response.getStatus().equals(SendRecvStatus.SUCCESS)) {
			return true;
		}
		return false;
	}

	public void configureMaps(Value[] values) throws TwisterException {
		execPlan.setMapConfigurations(values);
		boolean status = configureMapsInternal(values);
		if (!status) {
			if (jobConf.isFaultTolerance()) {
				System.out
						.println("Begin to handle Failure in configureMaps(Value[] values) ");
				handleFailures();
			} else {
				throw new TwisterException("ConfigureMaps failed.");
			}
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#configureMaps(cgl.imr.base.Value[])
	 */
	protected boolean configureMapsInternal(Value[] values)
			throws TwisterException {

		// double beginTime = System.currentTimeMillis();

		if (jobState.ordinal() >= JobState.MAP_CONFIGURING.ordinal()) {
			cleanupAndTerminateJob();
			throw new TwisterException("Maps can be configured only once.");
		}

		jobState = JobState.MAP_CONFIGURING;
		int numValues = values.length;
		if (numValues != numMapTasks) {
			throw new TwisterException(
					"Number of values[] should be equal to the number of map tasks.");
		}

		List<Integer> avaiableDaemons = faultDetector.getAvailableDeamons();
		int numAvailableDaemons = avaiableDaemons.size();

		MapperConf mapperConf = null;
		TaskAssignment mapAssignment = null;
		MapperRequest mapperRequest = null;
		for (int m = 0; m < numMapTasks; m++) {
			mapperConf = new MapperConf(m, values[m]);
			mapperRequest = new MapperRequest(jobConf, mapperConf,
					iterationCount);
			mapperRequest.setResponseTopic(responseTopic);
			mapAssignment = new TaskAssignment(mapperRequest,
					avaiableDaemons.get(m % numAvailableDaemons));
			mapTasksMap.put(new Integer(m), mapAssignment);
		}
		SendRecvResponse sendRecvResponse = null;
		// Map<Integer, WorkerResponse> responses = null;
		// WorkerResponse response = null;
		try {
			sendRecvResponse = sendAllExecutorRequestsAndReceiveResponses(
					mapTasksMap, numMapTasks);
		} catch (Exception e) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Could not send the map executor requests.", e);
		}
		if (sendRecvResponse != null
				&& sendRecvResponse.getStatus()
						.equals(SendRecvStatus.EXCEPTION)) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"ConfigureMaps produced errors at the daemons. Please see the logs for further information.");
		} else if (sendRecvResponse != null
				&& sendRecvResponse.getStatus().equals(SendRecvStatus.SUCCESS)) {
			this.mapConfigured = true;
			jobState = JobState.MAP_CONFIGURED;
			return true;
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#configureReduce(cgl.imr.base.Value[])
	 */
	public void configureReduce(Value[] values) throws TwisterException {

		int numValues = values.length;
		if (numValues < numReduceTasks) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Number of values[] is less than the numer of reduce tasks sepecified. "
							+ "It should be greater than or equal to the number of map tasks.");
		}

		execPlan.setReduceConfigurations(values);
		boolean status = configureReduceInternal(values);
		if (!status) {
			if (jobConf.isFaultTolerance()) {
				System.out
						.println("Begin to handle Failure in configureReduce(Value[] values)");
				handleFailures();
			} else {
				throw new TwisterException("ConfigureMaps failed.");
			}
		}
	}

	/**
	 * Used for configuring reduce tasks.
	 *
	 * @param values
	 * @throws TwisterException
	 */
	protected boolean configureReduceInternal(Value[] values)
			throws TwisterException {
		jobState = JobState.REDUCE_CONFIGURING;
		if (values != null) {
			int numValues = values.length;
			if (numValues != numReduceTasks) {
				throw new TwisterException(
						"Number of values[] should be equal to the number of reduce tasks.");
			}
		}

		List<Integer> avaiableDaemons = faultDetector.getAvailableDeamons();
		int numAvailableDaemons = avaiableDaemons.size();

		ReducerConf reducerConf = null;
		TaskAssignment reduceAssignment = null;
		ReducerRequest reudceExecutorRequest = null;
		String topicForReduceTask = null;
		for (int reduceTaskNo = 0; reduceTaskNo < numReduceTasks; reduceTaskNo++) {
			if (values != null) {
				reducerConf = new ReducerConf(reduceTaskNo,
						values[reduceTaskNo]);
			} else {
				reducerConf = new ReducerConf(reduceTaskNo);
			}
			topicForReduceTask = reduceTopicBase + reduceTaskNo;
			reudceExecutorRequest = new ReducerRequest(jobConf, reducerConf,
					topicForReduceTask, responseTopic, combineTopic,
					iterationCount);
			// /
			// TODO
			// Possible bug.
			// Reduce assignment is done based on the daemon no.
			// Then take the available deamons to use %

			reduceAssignment = new TaskAssignment(reudceExecutorRequest,
					avaiableDaemons.get(reduceTaskNo % numAvailableDaemons));
			reduceTasksMap.put(new Integer(reduceTaskNo), reduceAssignment);
		}
		SendRecvResponse sendRecvResponse = null;
		try {
			sendRecvResponse = sendAllExecutorRequestsAndReceiveResponses(
					reduceTasksMap, numReduceTasks);
		} catch (Exception e) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Could not send the reduce executor requests.", e);
		}

		if (sendRecvResponse != null
				&& sendRecvResponse.getStatus()
						.equals(SendRecvStatus.EXCEPTION)) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"ConfigureReduce produced errors at the daemons. Please see the logs for further information.");
			// responses = sendRecvResponse.getWorkerReponses();
			// Iterator<Integer> resIte = responses.keySet().iterator();
			// while (resIte.hasNext()) {
			// response = responses.get(resIte.next());
			// if (response.isHasException()) {
			// throw new TwisterException(
			// "ConfigureMaps produced errors. First error is "+
			// response.getExceptionString());
			// }
			// }

		} else if (sendRecvResponse != null
				&& sendRecvResponse.getStatus().equals(SendRecvStatus.SUCCESS)) {
			reduceConfigured = true;
			jobState = JobState.REDUCE_CONFIGURED;
			return true;
		}
		return false;

		//
		//
		// if (sendRecvResponse != null
		// && sendRecvResponse.getStatus()
		// .equals(SendRecvStatus.EXCEPTION)) {
		// cleanupAndTerminateJob();
		// responses = sendRecvResponse.getWorkerReponses();
		// Iterator<Integer> resIte = responses.keySet().iterator();
		// while (resIte.hasNext()) {
		// response = responses.get(resIte.next());
		// if (response.isHasException()) {
		// throw new TwisterException(
		// "Could not send all the map executor requests. First error is "
		// + response.getExceptionString());
		// }
		// }
		//
		// }

	}

	protected int getAssignedDaemonForTheMapTask(int number) {
		return mapTasksMap.get(new Integer(number)).getAssignedDaemon();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#getCurrentCombiner()
	 */
	public Combiner getCurrentCombiner() throws TwisterException {
		if (this.currentCombiner == null) {
			throw new TwisterException(
					"Combiner is not engaged. Please check if the combiner is "
							+ "specified in the JobConf.");
		}
		return this.currentCombiner;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#getMonitor()
	 */
	public TwisterMonitor getMonitor() {
		return this.monitor;
	}

	/**
	 * Gets the number of nodes from the nodes file.
	 *
	 * @return
	 * @throws IOException
	 */
	protected int getNumDaemonsFromConfigFiles() throws IOException {
		int count = 0;
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(mrConfig.getNodeFile()));
		while (reader.readLine() != null) {
			count++;
		}
		reader.close();
		return count * mrConfig.getDamonsPerNode();
	}

	// /**
	// * Currently use this static assignment. However, if this assignment does
	// * not work, the system will use another node. Extending this to a dynamic
	// * assignment strategy may help typical MapReduce computations. However,
	// * need to check the effect of a dynamic scheduling mechanism for the
	// * iterative MapReduce computations before changing.
	// *
	// * @param mapTaskNumber
	// * - Map task number.
	// * @return - Node number assigned to this map task.
	// */
	// protected int getDeamonNumberForMap(int mapTaskNumber) {
	// return mapTaskNumber % numDaemons;
	// }

	// /**
	// * Reduce tasks are assigned randomly to the nodes.
	// *
	// * @param ReduceTaskNumber
	// * - reduce task number
	// * @return - Randomly selected node.
	// */
	// protected int getDaemonNumberForReduce(int reduceTaskNumber) {
	// // return randomizer.nextInt(numDaemons);
	// return reduceTaskNumber % numDaemons;
	// }

	/**
	 * All the incoming messages are received here first, and next this method
	 * handles the requests appropriately.
	 */
	public void onEvent(byte[] message) {
		if (message != null) {
			if (message[0] == TwisterConstants.COMBINE_INPUT) {
				// System.out.println("Combining now!");
				// CombineInput combineInputTmp = new CombineInput();
				CombineInput combineInput = new CombineInput();
				try {
					// combineInputTmp.fromBytes(message);
					combineInput.fromBytes(message);
					if (this.currentCombiner != null) {
						if (!combineInput.isHasData()) {
							combineInput = getCombineInputFromRemoteHost(combineInput);
						}
						if (!combineInput.getOutputs().isEmpty()) {
							currentCombiner.combine(combineInput.getOutputs());
						}
						monitor.combinerInputReceived(combineInput);
					} else {
						logger.error("Combiner is not configured");
					}
					// System.out.println("Combining finishes!");
				} catch (Exception e) {
					// cleanupAndTerminateJob();
					// this.monitor.setMonitoringException(new TwisterException(
					// "Combiner encountered erros.", e));

					/*
					 * ZBJ: try to ignore the error here and use fault handler
					 * to restore the computation The exception here could be
					 * resulted by the error from the Daemons (died with unknown
					 * reason) However the client is still fine actually. Based
					 * on this reason ,terminating the job directly doesn't look
					 * proper.
					 */
					logger.error("Combiner encountered errors.");
					//logger.error("Combiner encountered errors.", e);
				}
			} else if (message[0] == TwisterConstants.TASK_STATUS) {
				this.monitor.onEvent(message);
			} else {
				// These are responses. So should go to response queue.
				try {
					WorkerResponse response = new WorkerResponse(message);
					//System.out.println("The response key is checking: " + response.getRefMessageId());
					responseMap.put(response.getRefMessageId(), response);
				} catch (Exception e) {
					cleanupAndTerminateJob();
					this.monitor.setMonitoringException(new TwisterException(
							"Error in receiving worker responses.", e));
					logger.error("Error in receiving worker responses.", e);
				}
			}
		}
	}

	private byte[] getDataFromServer(String host, int port, String key)
			throws TwisterException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			InetAddress addr = InetAddress.getByName(host);
			SocketAddress sockaddr = new InetSocketAddress(addr, port);

			// Create an unbound socket
			Socket sock = new Socket();

			// This method will block no more than timeout Ms.
			// If the timeout occurs, SocketTimeoutException is thrown.
			int timeoutMs = 20000; // 20 seconds
			sock.connect(sockaddr, timeoutMs);

			// Now send the stop signal to the TwisterDaemon
			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
					sock.getOutputStream()));
			wr.write(key + "\n");
			wr.flush();

			DataInputStream br = new DataInputStream(sock.getInputStream());
			byte[] buff = new byte[1048576];
			int len = 0;

			while ((len = br.read(buff)) > 0) {
				bout.write(buff, 0, len);
				// System.out.println("Reading bytes -------------- "+len);
			}
			wr.close();
			br.close();

			// System.out.println("Finished reading. recevied " + bout.size());
			sock.close();
			// Thread.sleep(100);
			bout.flush();

		} catch (Exception e) {
			throw new TwisterException("Error in downloading reduce input", e);
		}
		return bout.toByteArray();
	}

	private CombineInput getCombineInputFromRemoteHost(
			CombineInput combineInputTmp) throws TwisterException,
			SerializationException {
		// Map<Key,List<Value>> tmpMap=reduceInputTmp.getOutputs();
		StringValue memKey = null;// (StringValue)(tmpMap.get("known_key").get(0));//There
									// is only one value here.

		Map<Key, Value> tmpMap = combineInputTmp.getOutputs();
		Iterator<Key> ite = tmpMap.keySet().iterator();
		Key key = null;
		while (ite.hasNext()) {
			key = ite.next();
			memKey = (StringValue) tmpMap.get(key);
		}

		String[] parts = memKey.toString().split(":");
		// System.out.println(memKey);

		byte[] data = getDataFromServer(parts[0], Integer.parseInt(parts[1]),
				parts[2]);
		CombineInput combInput = new CombineInput();
		combInput.fromBytes(data);
		return combInput;
	}

	/**
	 * Try to partition tasks to available nodes. The logic simply try to assign
	 * tasks to nodes equally or nearly equally.
	 *
	 * @param values
	 *            - Array of values.
	 * @return
	 */
	protected List<Map<Key, Value>> partitionKeyValuesToMapTasks(
			List<KeyValuePair> pairs) {
		List<Map<Key, Value>> keyValueGroups = new ArrayList<Map<Key, Value>>();
		int numPairs = pairs.size();
		int perMap = numPairs / numMapTasks;
		int remainder = numPairs % numMapTasks;
		int currentMapSize = perMap;
		int offset = 0;

		Map<Key, Value> perMapTask = null;
		KeyValuePair pair = null;
		for (int i = 0; i < numMapTasks; i++) {
			currentMapSize = perMap;
			if (remainder > 0) {
				currentMapSize++;
				remainder--;
			}
			perMapTask = new HashMap<Key, Value>();
			for (int j = 0; j < currentMapSize; j++) {
				pair = pairs.get(offset + j);
				perMapTask.put(pair.getKey(), pair.getValue());
			}
			keyValueGroups.add(perMapTask);
			offset += currentMapSize;
		}
		return keyValueGroups;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#runMapReduce()
	 */
	public TwisterMonitor runMapReduce() throws TwisterException {

		boolean currentlyMonitoringMapReduce = false;
		synchronized (this) {
			if (isMonitoringMapReduce) {
				currentlyMonitoringMapReduce = true;
			} else {
				this.monitor.resetMonitor();
				isMonitoringMapReduce = true;
			}
		}
		if (currentlyMonitoringMapReduce) {
			throw new TwisterException(
					"A MapReduce computation is already running for this TwisterDriver. "
							+ "Use another driver for a new MapReduce computation.");
		}

		if (jobConf.isHasCombinerClass()) {
			this.configureCurrentCombiner();
		}

		if (!mapConfigured) {
			throw new TwisterException(
					"Map tasks are not configured. Please call configureMaps(..).");
		}

		if (!reduceConfigured) {
			configureReduceInternal(null);
		}

		MapTaskRequest mapRequest = null;
		try {
			for (int i = 0; i < numMapTasks; i++) {
				mapRequest = new MapTaskRequest(i, iterationCount);
				mapRequest.addKeyValue(new StringKey(jobConf.getJobId() + i),
						new IntValue(i));
				sendMapRequest(mapRequest);
			}
		} catch (Exception e) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Could not send all the map task requests.", e);
		} finally {
			iterationCount++;
		}
		jobState = JobState.MAP_SUBMITTED;
		return this.monitor;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#runMapReduce(java.util.List)
	 */
	public TwisterMonitor runMapReduce(List<KeyValuePair> pairs)
			throws TwisterException {

		setLastKeyValuePair(pairs);

		boolean currentlyMonitoringMapReduce = false;
		synchronized (this) {
			if (isMonitoringMapReduce) {
				currentlyMonitoringMapReduce = true;
			} else {
				this.monitor.resetMonitor();
				isMonitoringMapReduce = true;
			}
		}

		if (currentlyMonitoringMapReduce) {
			throw new TwisterException(
					"A MapReduce computation is already running for this TwisterDriver. "
							+ "Use another driver for a new MapReduce computation.");
		}

		if (!mapConfigured) {
			throw new TwisterException(
					"Map tasks are not configured. Please call configureMaps(..).");
		}

		if (jobConf.isHasCombinerClass()) {
			this.configureCurrentCombiner();
		}

		if (!reduceConfigured) {
			configureReduceInternal(null);
		}

		List<Map<Key, Value>> keyValueGroups = partitionKeyValuesToMapTasks(pairs);

		try {
			MapTaskRequest mapRequest = null;
			for (int i = 0; i < numMapTasks; i++) {
				mapRequest = new MapTaskRequest(i, iterationCount);
				mapRequest.setKeyValues(keyValueGroups.get(i));
				sendMapRequest(mapRequest);
			}
		} catch (Exception e) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Could not send all the map task requests.", e);
		} finally {
			iterationCount++;
		}
		jobState = JobState.MAP_SUBMITTED;
		return this.monitor;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#runMapReduceBCast(cgl.imr.base.Value)
	 */
	public TwisterMonitor runMapReduceBCast(Value val) throws TwisterException {
		setLastBcastValue(val);
		boolean currentlyMonitoringMapReduce = false;
		synchronized (this) {
			if (isMonitoringMapReduce) {
				// System.out.println("already in monitoring ");
				currentlyMonitoringMapReduce = true;
			} else {
				// System.out.println("new monitoring ");
				this.monitor.resetMonitor();
				isMonitoringMapReduce = true;
			}
		}

		if (currentlyMonitoringMapReduce) {
			throw new TwisterException(
					"A MapReduce computation is already running for this TwisterDriver. "
							+ "Use another driver for a new MapReduce computation.");
		}

		if (jobConf.isHasCombinerClass()) {
			this.configureCurrentCombiner();
		}

		if (!mapConfigured) {
			throw new TwisterException(
					"Map tasks are not configured. Please call configureMaps(..).");
		}

		if (!reduceConfigured) {
			configureReduceInternal(null);
		}

		MapTaskRequest mapRequest = null;
		try {
			// System.out.println("send Bcast value " + iterationCount);
			for (int i = 0; i < numMapTasks; i++) {
				mapRequest = new MapTaskRequest(i, iterationCount);
				mapRequest.addKeyValue(new StringKey(jobConf.getJobId() + i),
						val);
				sendMapRequest(mapRequest);
			}
		} catch (Exception e) {
			cleanupAndTerminateJob();
			throw new TwisterException(
					"Could not send all the map task requests.", e);
		} finally {
			iterationCount++;
		}
		jobState = JobState.MAP_SUBMITTED;
		return this.monitor;
	}

	protected Map<Integer, DaemonStatus> bcastNewJobRequestsAndReceiveResponses(
			PubSubMessage message, List<Integer> workingDaemons)
			throws PubSubException, SerializationException, TwisterException {

		Map<Integer, DaemonStatus> responses = new HashMap<Integer, DaemonStatus>();
		HashMap<Integer, String> refIds = new HashMap<Integer, String>();
		String refId = setRefMessage(message);

		for (int i : workingDaemons) {
			refIds.put(i, refId + i);
		}

		pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
				message.getBytes());
		Integer taskNo = null;
		int numDaemons = workingDaemons.size();
		// Keep checking for responses.
		boolean resReceived = false;
		boolean anyExceptions = false;
		boolean timeOut = false;
		long removeCount = 0;
		long waitedTime = 0;
		WorkerResponse response = null;
		Iterator<Integer> ite = null;
		while (!(resReceived || timeOut)) {
			ite = refIds.keySet().iterator();
			while (ite.hasNext()) {
				taskNo = ite.next();
				response = responseMap.remove(refIds.get(taskNo));
				if (response != null) {
					removeCount++;
					if (response.isHasException()) {
						anyExceptions = true;
						logger.error("New job request produced errors "
								+ response.getExceptionString());
					}
					//System.out.println("The Daemon I get " + taskNo);
					responses.put(taskNo,
							new DaemonStatus(true, System.currentTimeMillis()));
				}
			}
			// All responses received.
			if (removeCount == numDaemons) {
				resReceived = true;
			} else {
				// Wait and see.
				try {
					Thread.sleep(TwisterConstants.SEND_RECV_SLEEP_TIME);
				} catch (InterruptedException e) {
					logger.error(e);
				}
				waitedTime += TwisterConstants.SEND_RECV_SLEEP_TIME;
				//ZBJ: the last try to get all daemons
				if (waitedTime > TwisterConstants.SEND_RECV_NEWJOB_MAX_SLEEP_TIME) {
					// Timeout has reached. Check one more time and
					// consider the pending requests as failed.
					timeOut = true;
					ite = refIds.keySet().iterator();
					while (ite.hasNext()) {
						taskNo = ite.next();
						/*
						 * ZBJ: you won't need to wait for the daemon which you already got the response
						 * check responses to see if the reply is already there
						 */
						if (responses.get(taskNo) == null) {
							//System.out.println("The Daemon I am waiting "
									//+ taskNo);
							response = responseMap.remove(refIds.get(taskNo));
							if (response != null) {
								if (response.isHasException()) {
									anyExceptions = true;
									logger.error("New job request produced errors "
											+ response.getExceptionString());
								}
								responses.put(taskNo, new DaemonStatus(true,
										System.currentTimeMillis()));
							} else {
								//Finally, I still don't have those daemons
								logger.info("Desert Daemon " + taskNo);
								responses.put(taskNo, new DaemonStatus(false,
										System.currentTimeMillis()));
							}
						} else {
							// ZBJ: update the daemon status I already have in the last try
							// in order to avoid long time interval between updates
							// System.out.println("Update Daemon " + taskNo);
							responses.put(taskNo, new DaemonStatus(true,
									System.currentTimeMillis()));
						}
					}
				}
			}
		}

		if (anyExceptions) {
			throw new TwisterException("New job requested produced errors.");
		}
		return responses;
	}

	/**
	 * Broadcast a given message and receive responses.
	 *
	 * @param message
	 * @param numResponse
	 * @return
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	protected SendRecvResponse bcastRequestsAndReceiveResponses(
			PubSubMessage message) throws PubSubException,
			SerializationException {

		//ZBJ: record which response is received
		HashSet<String> receivedResponse = new HashSet<String>();
		HashMap<Integer, WorkerResponse> responses = new HashMap<Integer, WorkerResponse>();
		SendRecvResponse output = new SendRecvResponse();
		HashMap<Integer, String> refIds = new HashMap<Integer, String>();
		String refId = setRefMessage(message);

		for (int i : workingDaemons) {
			refIds.put(i, refId + i);
		}
		int numResponse = workingDaemons.size();

		pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
				message.getBytes());

		Integer taskNo = null;

		// Keep checking for responses.
		boolean resReceived = false;
		boolean anyExceptions = false;
		boolean anyFaults = false;
		boolean timeOut = false;
		int waitedCount = 0;
		long removeCount = 0;
		long waitedTime = 0;
		WorkerResponse response = null;
		Iterator<Integer> ite = null;
		while (!(resReceived || timeOut)) {
			ite = refIds.keySet().iterator();
			while (ite.hasNext()) {
				taskNo = ite.next();
				response = responseMap.remove(refIds.get(taskNo));
				if (response != null) {
					receivedResponse.add(refIds.get(taskNo));
					removeCount++;
					if (response.isHasException()) {
						anyExceptions = true;
						logger.error("Exceptions at daemons for broadcast operation: daemon no "
								+ response.getDaemonNo()
								+ " @ "
								+ response.getDaemonIp()
								+ " "
								+ response.getExceptionString());
					}
				}
			}
			// All responses received.
			if (removeCount == numResponse) {
				resReceived = true;
			} else {
				// Wait and see.
				try {
					Thread.sleep(TwisterConstants.SEND_RECV_SLEEP_TIME);
				} catch (InterruptedException e) {
					logger.error(e);
				}
				waitedTime += TwisterConstants.SEND_RECV_SLEEP_TIME;
				waitedCount++;
				if (jobConf.isFaultTolerance()
						&& (waitedCount % WAIT_COUNT_FOR_FAULTS) == 0) {
					if (faultDetector.isHasFault()) {
						anyFaults = true;
						break;
					}
				}
				if (waitedTime > TwisterConstants.SEND_RECV_MAX_SLEEP_TIME) {
					// Timeout has reached. Check one more time and
					// consider the pending requests as failed.
					timeOut = true;
					ite = refIds.keySet().iterator();
					while (ite.hasNext()) {
						taskNo = ite.next();
						//System.out.println("The response key is receiving: "
								//+ refIds.get(taskNo));

						if (!receivedResponse.contains(refIds.get(taskNo))) {
							System.out.println("The response from taksNo " + taskNo + " is not received");
							response = responseMap.remove(refIds.get(taskNo));
							if (response != null) {
								if (response.isHasException()) {
									anyExceptions = true;
									logger.error("Exceptions at daemons for broadcast operation: daemon no "
											+ response.getDaemonNo()
											+ " @ "
											+ response.getDaemonIp()
											+ " "
											+ response.getExceptionString());
								}
							} else {
								logger.error("No response for the message broadcast operation: task no "
										+ taskNo);
								anyFaults = true;
							}
						}

					}
				}
			}
		}

		output.setWorkerReponses(responses);
		if (anyFaults) {
			output.setStatus(SendRecvStatus.FALIURE);
			return output;
		}

		if (anyExceptions) {
			output.setStatus(SendRecvStatus.EXCEPTION);
			return output;
		}
		output.setStatus(SendRecvStatus.SUCCESS);
		return output;
	}

	/**
	 * Used for submitting <code>MapExecutorRequest</code> and
	 * <code> ReduceExecutorRequest</code> to the worker nodes. The method first
	 * sends all the requests and then wait till all the responses are received
	 * or a time out is reached.
	 *
	 * @param tasksMap
	 *            - A map of tasks. Key is the task number and the value is the
	 *            <code>TaskAssignment</code>. The assignment of tasks to nodes
	 *            is done by the caller.
	 * @param numTasks
	 *            - Number of tasks
	 * @return - A map of <code> Integer, WorkerResponse </code>.
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	protected SendRecvResponse sendAllExecutorRequestsAndReceiveResponses(
			ConcurrentHashMap<Integer, TaskAssignment> tasksMap, int numTasks)
			throws PubSubException, SerializationException {

		//ZBJ: record which response is received
		HashSet<String> receivedResponse = new HashSet<String>();
		HashMap<Integer, WorkerResponse> responses = new HashMap<Integer, WorkerResponse>();
		SendRecvResponse output = new SendRecvResponse();
		HashMap<Integer, String> refIds = new HashMap<Integer, String>();

		// Send map executor requests.
		TaskAssignment assignment = null;
		PubSubMessage request = null;
		Iterator<Integer> ite = tasksMap.keySet().iterator();
		String pubTopic = null;
		Integer taskNo = null;
		while (ite.hasNext()) {
			taskNo = ite.next();
			assignment = tasksMap.get(taskNo);
			request = assignment.getTaskRequest();
			refIds.put(taskNo, setRefMessage(request));
			pubTopic = TwisterConstants.MAP_REDUCE_TOPIC_BASE + "/"
					+ assignment.getAssignedDaemon();
			pubSubService.send(pubTopic, request.getBytes());
			//System.out.println("sending is done! " + taskNo + " " + assignment.getAssignedDaemon());
		}


		// Keep checking for responses.
		boolean resReceived = false;
		boolean anyExceptions = false;
		boolean anyFaults = false;
		boolean timeOut = false;
		int waitedCount = 0;
		long removeCount = 0;
		long waitedTime = 0;
		WorkerResponse response = null;

		while (!(resReceived || timeOut)) {
			ite = refIds.keySet().iterator();
			while (ite.hasNext()) {
				taskNo = ite.next();
				response = responseMap.remove(refIds.get(taskNo));
				if (response != null) {
					//System.out.println("response " + taskNo + " is got.");
					receivedResponse.add(refIds.get(taskNo));
					removeCount++;
					if (response.isHasException()) {
						anyExceptions = true;
						logger.error("Exceptions at daemons for send/recv operation: daemon no "
								+ response.getDaemonNo()
								+ " @ "
								+ response.getDaemonIp()
								+ " "
								+ response.getExceptionString());
					}
				}
			}
			// All responses received.
			if (removeCount == numTasks) {
				resReceived = true;
			} else {
				// Wait and see.
				try {
					Thread.sleep(TwisterConstants.SEND_RECV_SLEEP_TIME);
				} catch (InterruptedException e) {
					logger.error(e);
				}
				waitedTime += TwisterConstants.SEND_RECV_SLEEP_TIME;
				waitedCount++;
				if (jobConf.isFaultTolerance()
						&& (waitedCount % WAIT_COUNT_FOR_FAULTS) == 0) {
					if (faultDetector.isHasFault()) {
						anyFaults = true;
						break;
					}
				}
				if (waitedTime > TwisterConstants.SEND_RECV_MAPREQUEST_MAX_SLEEP_TIME) {
					// Timeout has reached. Check one more time and
					// consider the pending requests as failed.

					timeOut = true;
					ite = refIds.keySet().iterator();
					while (ite.hasNext()) {
						taskNo = ite.next();
						// ZBJ: only examine the taskno not got response yet
						if(!receivedResponse.contains(refIds.get(taskNo))) {
							response = responseMap.remove(refIds.get(taskNo));
							if (response != null) {
								if (response.isHasException()) {
									anyExceptions = true;
									logger.error("Exceptions at daemons for send/recv operation: daemon no "
											+ response.getDaemonNo()
											+ " @ "
											+ response.getDaemonIp()
											+ " "
											+ response.getExceptionString());
								}
							} else {
								logger.error("No response for the ExecutorRequest broadcast operation: task no "
										+ taskNo);
								anyFaults = true;
							}
						}
					}
				}
			}
		}

		output.setWorkerReponses(responses);
		if (anyFaults) {
			output.setStatus(SendRecvStatus.FALIURE);
			return output;
		}

		if (anyExceptions) {
			output.setStatus(SendRecvStatus.EXCEPTION);
			return output;
		}
		output.setStatus(SendRecvStatus.SUCCESS);
		return output;
	}

	protected void sendMapRequest(MapTaskRequest mapRequest)
			throws PubSubException, SerializationException {
		mapRequest.setJobId(jobConf.getJobId());
		mapRequest.setSinkBase(reduceTopicBase);
		mapRequest.setResponseTopic(responseTopic);
		pubSubService.send(TwisterConstants.MAP_REDUCE_TOPIC_BASE + "/"
				+ (getAssignedDaemonForTheMapTask(mapRequest.getMapTaskNo())),
				mapRequest.getBytes());
	}

	protected Map<Integer, DaemonStatus> sendNewJobRequest(String jobId,
			List<Integer> workingDaemons) throws PubSubException,
			SerializationException, TwisterException {

		NewJobRequest jobRequest = new NewJobRequest(jobId, this.responseTopic);

		// //
		// Have to get num daemons from the detector.

		// /
		return bcastNewJobRequestsAndReceiveResponses(jobRequest,
				workingDaemons);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see cgl.imr.client.TwisterModel#setMonitoringCompletes()
	 */
	public void setMonitoringCompletes() {
		synchronized (this) {
			this.isMonitoringMapReduce = false;
		}
	}

	protected String setRefMessage(PubSubMessage msg) {
		String refMsgId = uuidGen.generateTimeBasedUUID().toString();
		msg.setRefMessageId(refMsgId);
		return refMsgId;
	}

	/**
	 * Terminate the MapReduce computation and cleanup the driver.
	 *
	 * @throws TwisterException
	 */
	protected void terminate() throws TwisterException {

		EndJobRequest endMapReduceRequest = new EndJobRequest();
		endMapReduceRequest.setJobId(jobConf.getJobId());
		endMapReduceRequest.setResponseTopic(responseTopic);

		try {
			bcastRequestsAndReceiveResponses(endMapReduceRequest);

			this.faultDetector.close();
			this.pubSubService.close();
			jobState = JobState.TERMINATE_COMPLETES;
			shutDownHook.setTerminate();
			logger.info("MapReduce computation termintated gracefully.");
		} catch (Exception e) {
			logger.error("Could not send the teminate requests.", e);
		}
		//
		// jobState = JobState.TERMINATE_SUBMITTED;
		// Iterator<Integer> keys = responses.keySet().iterator();
		// WorkerResponse response = null;
		// while (keys.hasNext()) {
		// response = responses.get(keys.next());
		// if (response != null) {
		// if (response.isHasException()) {
		// cleanupAndTerminateJob();
		//
		//
		//
		// throw new TwisterException(
		// "Terminatation produced errors. First error is "
		// + response.getExceptionString());
		// }
		// }
		// }

		// try {
		// this.pubSubService.close();
		// } catch (PubSubException e) {
		// throw new TwisterException(e);
		// }

	}

	public void sendStartReduceMessage(
			ConcurrentHashMap<Integer, Integer> reduceInputMap)
			throws TwisterException {
		// Iterator<Integer> ite = reduceInputMap.keySet().iterator();
		// int key;
		// while(ite.hasNext()){
		// key=ite.next();
		// System.out.println(key+" "+reduceInputMap.get(key));
		// }
		StartReduceMessage msg = new StartReduceMessage(reduceInputMap,
				jobConf.getJobId(), reduceTopicBase);
		try {
			pubSubService.send(TwisterConstants.CLEINT_TO_WORKER_BCAST,
					msg.getBytes());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}

	public void setLastKeyValuePair(List<KeyValuePair> lastKeyValuePair) {
		this.lastKeyValuePair = lastKeyValuePair;
		this.lastBcastValue = null;
	}

	public List<KeyValuePair> getLastKeyValuePair() {
		return lastKeyValuePair;
	}

	public void setLastBcastValue(Value lastBcastValue) {
		this.lastBcastValue = lastBcastValue;
		this.lastKeyValuePair = null;
	}

	public Value getLastBcastValue() {
		return lastBcastValue;
	}

	// written by ZBJ, need for testing
	protected void rerunMapReduce() throws TwisterException {
		// ZBJ: try to restart the computation which has BcastValue
		if (getLastBcastValue() != null) {
			// System.out.println("Bcast Value resend");

			this.monitor.resetMonitor();

			if (jobConf.isHasCombinerClass()) {
				// System.out.println("no combiner?");
				this.configureCurrentCombiner();
				// System.out.println("set the combiner");
			}

			// ZBJ: It seems once this value is set as true, it is never turned
			// off.
			if (!mapConfigured) {
				throw new TwisterException(
						"Map tasks are not configured. Please call configureMaps(..).");
			}

			if (!reduceConfigured) {
				// System.out.println("no reducer?");
				configureReduceInternal(null);
			}

			MapTaskRequest mapRequest = null;
			try {
				for (int i = 0; i < numMapTasks; i++) {
					mapRequest = new MapTaskRequest(i, iterationCount);
					mapRequest.addKeyValue(
							new StringKey(jobConf.getJobId() + i),
							getLastBcastValue());
					sendMapRequest(mapRequest);
				}
			} catch (Exception e) {
				cleanupAndTerminateJob();
				throw new TwisterException(
						"Could not send all the map task requests.", e);
			} finally {
				iterationCount++;
			}
			jobState = JobState.MAP_SUBMITTED;

		} else if (getLastKeyValuePair() != null) {
			// ZBJ: try to restart the computation which has keyvalue pairs
			// System.out.println("KeyValuePair resend");
			this.monitor.resetMonitor();

			// ZBJ: It seems once this value is set as true, it is never turned
			// off.
			if (!mapConfigured) {
				throw new TwisterException(
						"Map tasks are not configured. Please call configureMaps(..).");
			}

			if (jobConf.isHasCombinerClass()) {
				// System.out.println("no combiner?");
				this.configureCurrentCombiner();
				// System.out.println("set the combiner");
			}

			if (!reduceConfigured) {
				// System.out.println("no reducer?");
				configureReduceInternal(null);
			}

			List<Map<Key, Value>> keyValueGroups = partitionKeyValuesToMapTasks(getLastKeyValuePair());
			try {
				MapTaskRequest mapRequest = null;
				for (int i = 0; i < numMapTasks; i++) {
					mapRequest = new MapTaskRequest(i, iterationCount);
					mapRequest.setKeyValues(keyValueGroups.get(i));
					sendMapRequest(mapRequest);
				}
			} catch (Exception e) {
				cleanupAndTerminateJob();
				throw new TwisterException(
						"Could not send all the map task requests.", e);
			} finally {
				iterationCount++;
			}
			jobState = JobState.MAP_SUBMITTED;
		}
	}
}
