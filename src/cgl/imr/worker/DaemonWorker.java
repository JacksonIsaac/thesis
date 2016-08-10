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

package cgl.imr.worker;

import java.io.File;
import java.io.FileFilter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.message.DirListRequest;
import cgl.imr.message.DirListResponse;
import cgl.imr.message.EndJobRequest;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.MemCacheClean;
import cgl.imr.message.MemCacheInput;
import cgl.imr.message.NewJobRequest;
import cgl.imr.message.ReduceInput;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.StartReduceMessage;
import cgl.imr.message.WorkerResponse;
import cgl.imr.types.IntKey;
import cgl.imr.util.CustomClassLoader;
import cgl.imr.util.JarClassLoaderException;

/**
 * Main entity that handles most of the server side functionality. DaemonWorker
 * accept messages coming from the pub-sub broker network and perform them
 * appropriately. To run map/reduce computations it uses the
 * <code>java.util.concurrent.Executor</code> functionality.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class DaemonWorker implements Subscribable {

	private static ConcurrentMap<String, CustomClassLoader> classLoaders = new ConcurrentHashMap<String, CustomClassLoader>();
	private static MemCache memCache = MemCache.getInstance();
	private static Logger logger = Logger.getLogger(DaemonWorker.class);

	public static CustomClassLoader getClassLoader(String jobId) {
		return classLoaders.get(jobId);
	}

	private TwisterConfigurations config;
	private String daemonCommTopic;
	private int daemonNo;
	private ConcurrentMap<String, ConcurrentMap<Integer, Mapper>> mappers;
	private PubSubService pubSubService;
	private ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>> reducers;
	private ConcurrentMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>> bcastReducers;
	ConcurrentHashMap<String, DataHolder> dataCache;
	private int daemonPort;

	private Executor taskExecutor = null;
	
	// ZBJ: this is for managing some threads for handling onEvent
	private ConcurrentMap<String, ConcurrentLinkedQueue<Thread>> onEventTasks;

	private String hostIP = null;
	private StatusNotifier notifer;
	
	public static int INITIAL_WAIT_TIME = 20;
	public static int SMALL_WAIT_COEFF = 2;
	public static int SMALL_WAIT_INTEVAL = 10;
	public static int LARGE_WAIT_INTEVAL = 1000;
	public static int MAX_WAIT_TIME = 60000;

	public DaemonWorker(int daemonNo, int numMapWorkers, ConcurrentHashMap<String, DataHolder> dataCache, int daemonPort,String hostIP)
			throws TwisterException {
		this.dataCache=dataCache;
		this.daemonPort=daemonPort;
		this.hostIP=hostIP;
		try {		
			this.config = TwisterConfigurations.getInstance();
		} catch (Exception e) {
			throw new TwisterException(e);
		}

		this.daemonNo = daemonNo;
		this.daemonCommTopic = TwisterConstants.MAP_REDUCE_TOPIC_BASE + "/"
				+ daemonNo;

		try {
			this.pubSubService = PubSubFactory.getPubSubService(config,
					EntityType.DAEMON, daemonNo);
			this.pubSubService.setSubscriber(this);
			this.pubSubService.subscribe(daemonCommTopic);
			this.pubSubService
					.subscribe(TwisterConstants.CLEINT_TO_WORKER_BCAST);
			notifer=new StatusNotifier(pubSubService,daemonNo,hostIP);
			notifer.start();
		} catch (PubSubException e) {
			if (this.pubSubService != null) {
				try {
					if(notifer!=null){
						notifer.stopNotifer();
					}
					this.pubSubService.close();
				} catch (PubSubException e1) { // Ignore
					logger.error("Failure in the Broker Connection. Terminating the daemon.");
					System.exit(-1);
				}
			}
			throw new TwisterException(e);
		}

		// Initialize the queues.
		this.mappers = new ConcurrentHashMap<String, ConcurrentMap<Integer, Mapper>>();
		this.reducers = new ConcurrentHashMap<String, ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>>>();
		bcastReducers=new ConcurrentHashMap<String, ConcurrentMap<String,ConcurrentLinkedQueue<Reducer>>>();
		
		taskExecutor = Executors.newFixedThreadPool(numMapWorkers);
		
		onEventTasks = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Thread>>();
		
		//taskExecutor = Executors.newCachedThreadPool();
		logger.info("Daemon no: " + daemonNo + " started with "+ numMapWorkers+ " workers.");
	}

	/**
	 * Helper method to create the partition file using a distribution of input
	 * files available across the cluster. List all the file names that matches
	 * the given filter pattern and sends a response.
	 * 
	 * @param msg
	 *            - A DirListRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	private void handleDirList(byte[] msg) throws TwisterException,
			PubSubException, SerializationException {
		DirListRequest listRequest = new DirListRequest();
		listRequest.fromBytes(msg);
		File[] files = null;
		List<String> selectedFiles = new ArrayList<String>();
		File dir = new File(listRequest.getDirectry());
		if (!dir.exists()) {
			logger.warn("Requested directory: " + dir.getName()
					+ " does not exist.");
			WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
			response.setRefMessageId(listRequest.getRefMessageId());
			response.setExceptionString(listRequest.getDirectry()
					+ " directory does not exist");
			this.pubSubService.send(listRequest.getResponseTopic(), response
					.getBytes());
		} else {
			FileFilter fileFilter = new FileFilter() {
				public boolean accept(File file) {
					return !(file.isDirectory());
				}
			};
			files = dir.listFiles(fileFilter);
			if (files != null) {
				for (File file : files) {
					if (file.getName().contains(listRequest.getFileFilter())) {
						selectedFiles.add(file.getAbsolutePath());
					}
				}
			}
			DirListResponse response = new DirListResponse(selectedFiles,
					daemonNo, hostIP);
			this.pubSubService.send(listRequest.getResponseTopic(), response
					.getBytes());
		}
	}

	private class HandleMapperRequestThread implements Runnable {
		private byte[] request;

		HandleMapperRequestThread(byte[] req) {
			request = new byte[req.length];
			for (int i = 0; i < req.length; i++) {
				request[i] = req[i];
			}
		}

		@Override
		public void run() {
			try {
				/*
				double beginTime = System.currentTimeMillis();
				System.out.println("begin handling MapperRequest, Daemon "
						+ daemonNo);
				*/
				
				//ZBJ: there is a little repeat here
				MapperRequest mapperRequest = new MapperRequest(request);
				JobConf jobConf = mapperRequest.getJobConf();
				
				// Create the response object.
				WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
				response.setRefMessageId(mapperRequest.getRefMessageId());

				CustomClassLoader classLoader = classLoaders.get(jobConf
						.getJobId());
				if (classLoader != null) {
					Mapper exec = new Mapper(mapperRequest, pubSubService,
							classLoader, dataCache, daemonPort, hostIP);
					
					//ZBJ: synchronize the processing of MapperRequest, since it is concurrent now
					synchronized (mappers) {
						ConcurrentMap<Integer, Mapper> mapperMap = mappers
								.get(jobConf.getJobId());
						if (mapperMap == null) {

							//mapperMap = mappers.get(jobConf.getJobId());
							//if (mapperMap == null) {
								mapperMap = new ConcurrentHashMap<Integer, Mapper>();
								mappers.put(jobConf.getJobId(), mapperMap);
							//}
						}
						mapperMap
								.put(new Integer(mapperRequest.getMapTaskNo()),
										exec);
						
						// ZBJ: some output test
						/*
						 * String str = "MapperRequest on Daemon " + daemonNo +
						 * ": " + mapperRequest.getMapTaskNo(); str = str +
						 * "\nMap Tasks on Daemon " + daemonNo + ": ";
						 * for(Integer i : mapperMap.keySet()) { str = str + i+
						 * " "; } System.out.println(str);
						 */
					}
				} else {
					response.setExceptionString("Invalid job Id. No class loader configured.");
				}
				pubSubService.send(mapperRequest.getResponseTopic(),
						response.getBytes());

				/*
				System.out.println("finish handling MapperRequest, Daemon "
						+ daemonNo + " "
						+ (System.currentTimeMillis() - beginTime));
				*/
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			request = null;
		}
	}
	
	/**
	 * Creates a Mapper to this particular map task. The mappers are stored
	 * (cached) till the termination of that particular MapReduce computation.
	 * Sends a response to the client.
	 * 
	 * @param request
	 *            - A MapperRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	public void handleMapperRequest(byte[] request) throws TwisterException,
			PubSubException, SerializationException {
		
		MapperRequest mapperRequest = new MapperRequest(request);
		JobConf jobConf = mapperRequest.getJobConf();
		
		// ZBJ: create a thread to handle mapper request
		// since it could take long time to load the file in mapper configuration
		// we have to wait.
		HandleMapperRequestThread handler = new HandleMapperRequestThread(request);
		Thread t = new Thread(handler);
		
		ConcurrentLinkedQueue<Thread> tQueue = null;
		synchronized (onEventTasks) {
			if(this.onEventTasks.get(jobConf.getJobId()) == null) {
				tQueue  = new ConcurrentLinkedQueue<Thread>();
			} else {
				 tQueue = this.onEventTasks.get(jobConf.getJobId());
			}
			tQueue.add(t);
			this.onEventTasks.put(jobConf.getJobId(), tQueue);
		}
		
		t.start();
		/*
		double beginTime = System.currentTimeMillis();
		System.out.println("begin handling MapperRequest, Daemon " + daemonNo);
		MapperRequest mapperRequest = new MapperRequest(request);
		JobConf jobConf = mapperRequest.getJobConf();
		// Create the response object.
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		response.setRefMessageId(mapperRequest.getRefMessageId());

		CustomClassLoader classLoader = classLoaders.get(jobConf.getJobId());
		if (classLoader != null) {
			Mapper exec = new Mapper(mapperRequest, this.pubSubService,
					classLoader,dataCache,daemonPort,hostIP);
			ConcurrentMap<Integer, Mapper> mapperMap = this.mappers.get(jobConf
					.getJobId());
			if (mapperMap == null) {
				synchronized (this) {
					mapperMap = this.mappers.get(jobConf.getJobId());
					if (mapperMap == null) {
						mapperMap = new ConcurrentHashMap<Integer, Mapper>();
						mappers.put(jobConf.getJobId(), mapperMap);
					}
				}
			}
			mapperMap.put(new Integer(mapperRequest.getMapTaskNo()), exec);
		} else {
			response
					.setExceptionString("Invalid job Id. No class loader configured.");
		}
		this.pubSubService.send(mapperRequest.getResponseTopic(), response
				.getBytes());
		
		System.out.println("finish handling MapperRequest, Daemon " + daemonNo + " " + (System.currentTimeMillis() - beginTime));
		*/
	}

	/**
	 * Removes the cached mappers and the reducers and send a response to the
	 * client.
	 * 
	 * @param request
	 *            - An EndMapReduceRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 * @throws InterruptedException 
	 */
	public void handleMapReduceTermination(byte[] request)
			throws TwisterException, PubSubException, SerializationException, InterruptedException {
		
		//ZBJ: some output test
		// System.out.println("begin handling MapReduceTermination, Daemon "
		// + daemonNo);
		
		EndJobRequest endIterations = new EndJobRequest();
		endIterations.fromBytes(request);
		String jobId = endIterations.getJobId();
		
		//ZBJ: try to wait those threads to die, if termination required by FaultHandler in the step
		// of map configuration, we have to wait the file loading threads to die naturally...
		ConcurrentLinkedQueue<Thread> tQueue = this.onEventTasks.get(jobId);
		while(!tQueue.isEmpty()) {
			tQueue.poll().join();
		}
		
		//ZBJ: clean the Queue in onEvenTasks, there may be other jobids
		this.onEventTasks.remove(jobId);
		
		ConcurrentMap<Integer, Mapper> mapperMap = mappers.get(jobId);
		if (mapperMap != null) {
			Mapper mapper = null;
			Integer key = null;
			Iterator<Integer> ite = mapperMap.keySet().iterator();
			while (ite.hasNext()) {
				key = ite.next();
				mapper = mapperMap.get(key);
				if (mapper != null) {
					mapper.close();
				}
				mapperMap.remove(key);
			}
			mappers.remove(jobId);
		}

		Map<String, ConcurrentLinkedQueue<Reducer>> reduceExecutorMap = this.reducers.get(jobId);
		String reduceTopic = null;	
		Reducer reducer = null;
		if (reduceExecutorMap != null) {
			Iterator<String> keys = reduceExecutorMap.keySet().iterator();
			while (keys.hasNext()) {
				reduceTopic = keys.next();
				ConcurrentLinkedQueue<Reducer> reducers = reduceExecutorMap.get(reduceTopic);
				Iterator<Reducer> ite=reducers.iterator();
				while(ite.hasNext()){
					ite.next().terminate();
				}
				this.pubSubService.unsubscribe(reduceTopic);
				reducers.clear();
			}
			reduceExecutorMap.clear();
			reducers.remove(jobId);
			reduceExecutorMap = null;
		}
		//By this time all the reducers have been terminated. We only need to check if there are any
		//reducers registered as bcast reducers.
		reduceExecutorMap = this.bcastReducers.get(jobId);
		if (reduceExecutorMap != null) {
			Iterator<String> keys = reduceExecutorMap.keySet().iterator();
			while (keys.hasNext()) {
				reduceTopic = keys.next();				
				this.pubSubService.unsubscribe(reduceTopic);
				
			}
			reduceExecutorMap.clear();
			bcastReducers.remove(jobId);
			reduceExecutorMap = null;
		}
		

		// Remove the class loader.
		CustomClassLoader classLoader = classLoaders.get(jobId);
		if (classLoader != null) {
			classLoader.close();
			classLoaders.remove(jobId);
			classLoader = null;
		} else {
			logger.warn("Termination request received for invalid jobId.");
		}

		// Remove memCahce objects if any.
		memCache.remove(jobId);

		// Send a response message ...
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		// Client expects the daemonNo to be added to the refId.
		response.setRefMessageId(endIterations.getRefMessageId() + daemonNo);
		this.pubSubService.send(endIterations.getResponseTopic(), response
				.getBytes());

		Runtime.getRuntime().gc();
		
		// ZBJ: output test
		// System.out.println("finish handling MapReduceTermination, Daemon " +
		// daemonNo);
	}

	/**
	 * Schedules the execution of a map task by finding appropriate mapper
	 * object.
	 * 
	 * @param request
	 *            - A MapTaskRequest.
	 * @throws TwisterException
	 * @throws SerializationException
	 */
	public void handleMapTask(byte[] request) throws TwisterException,
			SerializationException {
		
		MapTaskRequest mapRequest = new MapTaskRequest(request);
		ConcurrentMap<Integer, Mapper> mapperMap = mappers.get(mapRequest
				.getJobId());
		Mapper exec = mapperMap.get(mapRequest.getMapTaskNo());
		

		if (exec != null) {
			exec.setCurrentRequest(mapRequest);
			taskExecutor.execute(exec);
		} else {
			logger
					.error("No mapper is registered for this map task " + mapRequest.getMapTaskNo() + ". @ the daemon no: "
							+ daemonNo);
		}
		
	}

	/**
	 * Initializing the daemon for a new MapReduce computation. This will create
	 * a new class loader for this job and store it in a hash table for later
	 * use.
	 * 
	 * @param message
	 *            - Set of bytes for NewJobRequest message.
	 * @throws SerializationException
	 * @throws PubSubException
	 */
	private void handleNewJobRequest(byte[] message)
			throws SerializationException, PubSubException {
		NewJobRequest newJobRequest = new NewJobRequest(message);
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);

		// Client expects the daemonNo to be added to the refId.
		response.setRefMessageId(newJobRequest.getRefMessageId() + daemonNo);

		CustomClassLoader classLoader = null;
		try {
			classLoader = new CustomClassLoader();
			classLoaders.put(newJobRequest.getJobId(), classLoader);
		} catch (JarClassLoaderException e) {
			response.setExceptionString("Could not initiate the class loader.");
			logger.error(e);
		}

		this.pubSubService.send(newJobRequest.getResponseTopic(), response
				.getBytes());
	}

	/**
	 * Collect map outputs to the reduce task and schedules the execution if all
	 * the expected map outputs are received.
	 * 
	 * @param msg
	 * @throws TwisterException
	 * @throws SerializationException
	 */

	//private int redCount = 0;

	private void handleReduceInput(byte[] msg) throws TwisterException,
			SerializationException {
		ReduceInput reduceInput = new ReduceInput(msg);		
		Map<String, ConcurrentLinkedQueue<Reducer>> rwMap = this.reducers.get(reduceInput.getJobId());
		boolean reduceRequestHandled=false;
		if (rwMap == null) {
			logger
					.error("No reducer is registered for this reduce input. @ the daemon no: "
							+ daemonNo);
		}
		ConcurrentLinkedQueue<Reducer> reduceExecutors = rwMap.get(reduceInput.getSink());
		if(reduceExecutors!=null){
			Iterator<Reducer> ite=reduceExecutors.iterator();
			while(ite.hasNext()){			
				ite.next().handleReduceInputMessage(reduceInput);
			}
			reduceRequestHandled=true;
		}
		
		//Bcast reducers.
		rwMap=null;
		rwMap=bcastReducers.get(reduceInput.getJobId());
		Reducer reducer=null;
		if(rwMap!=null){
			reduceExecutors = rwMap.get(reduceInput.getSink());
			if(reduceExecutors!=null){
				Iterator<Reducer> ite=reduceExecutors.iterator();
				while(ite.hasNext()){	
					reducer=ite.next();
					reducer.handleReduceInputMessageForBcast(reduceInput,new IntKey(reducer.getReducerNo()));
					//System.out.println("Calling reducer "+reduceInput.getSink()+"  "+reduceInput.getIteration());
				}
				reduceRequestHandled=true;
			}
			
		}		
		
		if(!reduceRequestHandled){
			logger.error("Reduce input is not handled. There are not reduce tasks expecting this input.");
		}
		
//		if (reduceExecutor.isAllReduceInputsReceived()) {
//			taskExecutor.execute(reduceExecutor);
//		}
		//redCount++;
	}

	/**
	 * Starts a Reducer to handle the reduce computation. Reducer is also cached
	 * and terminated at the end of the MapReduce computation. Sends a response
	 * to the client.
	 * 
	 * @param request
	 *            - A ReducerRequest.
	 * @throws TwisterException
	 * @throws PubSubException
	 * @throws SerializationException
	 */
	public void handleReducerRequest(byte[] request) throws TwisterException,
			PubSubException, SerializationException {
		//redCount = 0;
		ReducerRequest reduceRequest = new ReducerRequest(request);
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		response.setRefMessageId(reduceRequest.getRefMessageId());
		JobConf jobConf=reduceRequest.getJobConf();
		try {
			CustomClassLoader classLoader = classLoaders.get(reduceRequest
					.getJobConf().getJobId());
			ConcurrentLinkedQueue<Reducer> reduceExecs;
			if (classLoader != null) {
				Reducer reduceExecutor = new Reducer(this.pubSubService,
						reduceRequest, classLoader,dataCache,daemonPort,hostIP);
				ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>> reduceExecMap = this.reducers
						.get(jobConf.getJobId());
				if (reduceExecMap == null) {
					reduceExecMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Reducer>>();
					reduceExecs=new ConcurrentLinkedQueue<Reducer>();
					reduceExecs.add(reduceExecutor);
					reduceExecMap.put(reduceRequest.getReduceTopic(),reduceExecs);
					this.reducers.put(jobConf.getJobId(),reduceExecMap);
				} else {
					if(reduceExecMap.containsKey(reduceRequest.getReduceTopic())){
						reduceExecs=reduceExecMap.get(reduceRequest.getReduceTopic());
						reduceExecs.add(reduceExecutor);
					}else{
						reduceExecs=new ConcurrentLinkedQueue<Reducer>();
						reduceExecs.add(reduceExecutor);
						reduceExecMap.put(reduceRequest.getReduceTopic(),reduceExecs);
					}					
				}
				this.pubSubService.subscribe(reduceRequest.getReduceTopic());
				if(reduceRequest.getJobConf().isRowBCastSupported()){
					//String rowBCastTopic=jobConf.getRowBCastTopic()+(reduceRequest.getReduceConf().getReduceTaskNo()%jobConf.getSqrtReducers());
									
					String bcastTopic=jobConf.getRowBCastTopic()+reduceRequest.getReduceConf().getReduceTaskNo()/jobConf.getSqrtReducers();
					//System.out.println("Adding to bcast topic: "+bcastTopic);
					reduceExecMap = this.bcastReducers.get(jobConf.getJobId());
					if (reduceExecMap == null) {
						reduceExecMap = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Reducer>>();
						reduceExecs=new ConcurrentLinkedQueue<Reducer>();
						reduceExecs.add(reduceExecutor);
						reduceExecMap.put(bcastTopic,reduceExecs);
						this.bcastReducers.put(jobConf.getJobId(),reduceExecMap);
					} else {
						if(reduceExecMap.containsKey(bcastTopic)){
							reduceExecs=reduceExecMap.get(bcastTopic);
							reduceExecs.add(reduceExecutor);
						}else{
							reduceExecs=new ConcurrentLinkedQueue<Reducer>();
							reduceExecs.add(reduceExecutor);
							reduceExecMap.put(bcastTopic,reduceExecs);
						}					
					}					
					this.pubSubService.subscribe(bcastTopic);
					
				}
			} else {
				response
						.setExceptionString("Invalid job id. No class loader is set.");
			}
		} catch (TwisterException mre) {
			response.setExceptionString(mre.getMessage());
			this.pubSubService.send(reduceRequest.getResponseTopic(), response
					.getBytes());
		}

		this.pubSubService.send(reduceRequest.getResponseTopic(), response
				.getBytes());
	}

	/**
	 * Listening method for all the incoming messages from the pub-sub broker
	 * network.
	 */
	public void onEvent(byte[] message) {

		if (message != null) {
			try {
				switch (message[0]) {
				case TwisterConstants.DIR_LIST_REQ:
					handleDirList(message);
					break;
				case TwisterConstants.NEW_JOB_REQUEST:
					handleNewJobRequest(message);
					break;
				case TwisterConstants.MAPPER_REQUEST:
					handleMapperRequest(message);
					break;
				case TwisterConstants.REDUCE_WORKER_REQUEST:
					handleReducerRequest(message);
					break;
				case TwisterConstants.MAP_TASK_REQUEST:
					handleMapTask(message);
					break;
				case TwisterConstants.REDUCE_INPUT:
					handleReduceInput(message);
					break;
				case TwisterConstants.MAP_ITERATIONS_OVER:
					handleMapReduceTermination(message);
					break;
				case TwisterConstants.MEMCACHE_INPUT:
					handleMemCacheInput(message);
					break;
				case TwisterConstants.MEMCACHE_CLEAN:
					handleMemCacheClean(message);
					break;					
				case TwisterConstants.START_REDUCE:
					handleStartReduce(message);
					break;
				default:
					logger
							.error("Invalid message received by the DaemonWorker.");
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	private void handleStartReduce(byte[] message)
			throws SerializationException {
		StartReduceMessage msg = new StartReduceMessage(message);
		ConcurrentMap<String, ConcurrentLinkedQueue<Reducer>> reduceExecMap = this.reducers
				.get(msg.getJobId());
		if (reduceExecMap != null) {

			Iterator<String> ite = reduceExecMap.keySet().iterator();
			ConcurrentLinkedQueue<Reducer> reducers;
			int reducerNo;
			boolean allReceived = false;
			boolean timeOut = false;
			Reducer reducer;
			while (ite.hasNext()) {
				allReceived = false;
				timeOut = false;
				reducers = reduceExecMap.get(ite.next());
				Iterator<Reducer> reduceIterator=reducers.iterator();
				while (reduceIterator.hasNext()) {
					reducer=reduceIterator.next();
					reducerNo = reducer.getReducerNo();
					int numExpectedInputs = msg
							.getNumReduceInputsExpected(reducerNo);
					int totalSleeps = 0;
					if (reducer.getNumReduceInputsReceived() == numExpectedInputs) {					
						taskExecutor.execute(reducer);
					} else {
						int sleep = INITIAL_WAIT_TIME;
						int sleepCount = 1;
						while (!(allReceived || timeOut)) {
							if (reducer.getNumReduceInputsReceived() == numExpectedInputs) {
								allReceived = true;
								break;
							} else {
								try {
									Thread.sleep(sleep);
									totalSleeps += sleep;
									if (totalSleeps >= MAX_WAIT_TIME) {
										timeOut = true;
									}
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								
								//calculate next sleep time
								//System.out.println("TotalSleep " + reducerNo + " " +  totalSleeps + " " + sleepCount);
								if(totalSleeps > LARGE_WAIT_INTEVAL) {
									sleep = LARGE_WAIT_INTEVAL;
								} else {
									//generate the total sleep time till next wake up
									//assume this value will be larger than INITIAL_WAIT_TIME
									//now the first value generated is 20, which is larger than INITIAL_WAIT_TIME 2
									 sleepCount++;
									int nextTotalSleep = (int) (Math.pow(SMALL_WAIT_COEFF, sleepCount)*SMALL_WAIT_INTEVAL);
									
									//calculate the sleep time between 2 totalSleeps values.
									 sleep =  nextTotalSleep - totalSleeps; 
								}
							}
						}
						if (allReceived) {
							taskExecutor.execute(reducer);							
						}
						if (timeOut) {
							logger
									.error("Reduce Task "
											+ reducerNo
											+ " did not receive all the inputs. So timeout occurs.");
						}
					}
				}
			}
		}
		//System.out.println("DataCache Size: " + this.dataCache.size());
	}

	/**
	 * Remove an object from the memcache.
	 * 
	 * @param message
	 * @throws SerializationException
	 */
	private void handleMemCacheClean(byte[] message)
			throws SerializationException {
		MemCacheClean cleanRequest = new MemCacheClean(message);
		memCache.remove(cleanRequest.getJobId(), cleanRequest.getKey());
	}

	/**
	 * Adds a Value type data object to memcache.
	 * 
	 * @param message
	 * @throws SerializationException
	 * @throws PubSubException
	 */
	private void handleMemCacheInput(byte[] message)
			throws SerializationException, PubSubException {
		MemCacheInput input = new MemCacheInput(message);
		memCache.add(input.getJobId(), input.getKey(), input.getValue());
		WorkerResponse response = new WorkerResponse(daemonNo, hostIP);
		// Client expects the daemonNo to be added to the refId.
		response.setRefMessageId(input.getRefMessageId() + this.daemonNo);
		this.pubSubService.send(input.getResponseTopic(), response.getBytes());
	}

	/**
	 * Terminate the DaemonWorker.
	 * 
	 * @throws TwisterException
	 */
	public void termintate() throws TwisterException {
		try {
			Thread.sleep(5000);// wait for few seconds
		} catch (InterruptedException e) {
			throw new TwisterException(e);
		}
		try {
			this.notifer.stopNotifer();
			this.pubSubService.close();			
		} catch (PubSubException e) {
			logger.error("Failure in the Broker Connection. Terminating the daemon.");
			System.exit(-1);
			//throw new TwisterException(e);
		}
	}
	
//	private String getIP() throws TwisterException {
//		Enumeration<NetworkInterface> netInterfaces = null;
//
//		try {
//			netInterfaces = NetworkInterface.getNetworkInterfaces();
//		} catch (SocketException e) {
//			throw new TwisterException(e);
//		}
//
//		while (netInterfaces.hasMoreElements()) {
//			NetworkInterface ni = netInterfaces.nextElement();
//			boolean isNISatisfied = false;
//			try {
//				isNISatisfied = !ni.isLoopback() && !ni.isPointToPoint()
//						&& !ni.isVirtual() && ni.isUp();
//			} catch (SocketException e) {
//				throw new TwisterException(e);
//			}
//			
//			if (isNISatisfied) {
//				Enumeration<InetAddress> address = ni.getInetAddresses();
//				while (address.hasMoreElements()) {
//					InetAddress addr = address.nextElement();
//					boolean isIPSatisfied = !addr.isLoopbackAddress()
//							&& !addr.isSiteLocalAddress()
//							&& !addr.isAnyLocalAddress()
//							&& !addr.isLinkLocalAddress()
//							&& !addr.isMulticastAddress()
//							&& !(addr.getHostAddress().indexOf(":") > -1);
//					if (isIPSatisfied) {
//						return addr.getHostAddress();
//					}
//				}
//			}
//		}
//
//		String hostIP = null;
//		try {
//			hostIP = InetAddress.getLocalHost().getHostAddress();
//		} catch (UnknownHostException e) {
//			throw new TwisterException(e);
//		}
//		return hostIP;
//	}
}
