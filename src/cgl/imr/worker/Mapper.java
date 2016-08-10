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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.PubSubService;
import cgl.imr.base.ReducerSelector;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapOutputCollectorImpl;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.message.MapTaskRequest;
import cgl.imr.message.MapperRequest;
import cgl.imr.message.ReduceInput;
import cgl.imr.message.TaskStatus;
import cgl.imr.types.StringKey;
import cgl.imr.types.StringValue;
import cgl.imr.util.CustomClassLoader;

/**
 * Executor for the map tasks. Executor holds the map task configuration and the
 * <code>DaemonWorker</code> schedules them for execution depending on the
 * requests from the <code>TwisterDriver</code>.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class Mapper implements Runnable {

	private static Logger logger = Logger.getLogger(Mapper.class);
	
	private CustomClassLoader classLoader;

	private MapTaskRequest currentRequest = null;
	private Exception exception;
	private boolean hasException = false;

	private JobConf jobConf;
	private MapTask mapTask;
	// private int iteration;

	private int mapTaskNo;
	private PubSubService pubsubService;
	private int daemonPort;
	private String hostIP;

	ConcurrentHashMap<String, DataHolder> dataCache;

	public Mapper(MapperRequest mapperRequest, PubSubService pubsubService,
			CustomClassLoader classLoader,
			ConcurrentHashMap<String, DataHolder> dataCache, int daemonPort,
			String hostIP) throws TwisterException {
		this.hostIP = hostIP;
		this.dataCache = dataCache;
		this.daemonPort = daemonPort;
		this.classLoader = classLoader;
		this.pubsubService = pubsubService;
		this.mapTaskNo = mapperRequest.getMapTaskNo();
		// this.iteration= mapperRequest.getIteration();
		this.jobConf = mapperRequest.getJobConf();
		MapperConf mapperConf = mapperRequest.getMapConf();
		Class<?> c;
		String className;
		try {
			className = jobConf.getMapClass();
			c = Class.forName(className, true, classLoader);
			this.mapTask = (MapTask) c.newInstance();
			this.mapTask.configure(jobConf, mapperConf);
		} catch (Exception e) {
			throw new TwisterException("Could not instantiate the Mapper.", e);
		}
	}

	public void close() throws TwisterException {
		if (this.mapTask != null) {
			mapTask.close();
		}
	}

	public MapTaskRequest getCurrentRequest() {
		return currentRequest;
	}

	public Exception getException() {
		return exception;
	}

	private ReducerSelector getReducerSelector(String reducerSink)
			throws TwisterException {
		Class<?> c;
		try {
			c = Class.forName(jobConf.getReducerSelectorClass(), true,
					classLoader);
			ReducerSelector reducerSelector = (ReducerSelector) c.newInstance();
			reducerSelector.configure(jobConf, reducerSink);
			return reducerSelector;
		} catch (Exception e) {
			throw new TwisterException("Could not load reducer selector.", e);
		}

	}

	public synchronized boolean isHasException() {
		return hasException;
	}

	public void run() {
		long beginTime = 0;
		int iteration = 0;
		try {
			if (currentRequest == null) {
				throw new TwisterException("No map request to execute.");
			}

			iteration = currentRequest.getIteration();
			// if(currentRequest.getMapTaskNo()==0){
			// System.out.println("MAP EXECUTING @ iteration "+iteration);
			// }

			ReducerSelector reducerSelector = getReducerSelector(currentRequest
					.getSinkBase());
			// MapOutputCollector collector= new
			// MapOutputCollectorImpl(reducerSelector, iteration);
			MapOutputCollector collector;
			if (jobConf.isRowBCastSupported()) {
				collector = new MapOutputCollectorImpl(reducerSelector,
						iteration, jobConf.getRowBCastTopic(), jobConf
								.getSqrtReducers());
			} else {
				collector = new MapOutputCollectorImpl(reducerSelector,
						iteration);
			}

			beginTime = System.currentTimeMillis();
			Map<Key, Value> keyValueMap = currentRequest.getKeyValues();
			Iterator<Key> ite = keyValueMap.keySet().iterator();
			Key key;
			while (ite.hasNext()) {
				key = ite.next();
				mapTask.map(collector, key, keyValueMap.get(key));
			}
			long endTime = System.currentTimeMillis();
			// if(currentRequest.getMapTaskNo()==0){
			// System.out.println("MAP TASK 0 TOOK "+((double)(endTime -
			// beginTime))/1000+" Seconds.");
			// }
			TaskStatus status = new TaskStatus(TwisterConstants.MAP_TASK,
					TwisterConstants.SUCCESS, mapTaskNo, (endTime - beginTime),
					iteration);

			status.setReduceInputMap(collector.getReduceInputMap());

			if (jobConf.isHasReduceClass()) {
				// Now we have the collector filed with values. Simply iterate
				// over it and publish the bytes to the respective reducer
				// topic.
				List<ReduceInput> inputList = collector.getReduceInputs();
				for (ReduceInput input : inputList) {
					// System.out.println("Sending :"+mapTaskNo+
					// "  to "+input.getSink());
					// One map output goes to one reducer
					ReduceInput newInput = copyDataToCacheIfLargeAndGetReduceInput(
							input, 1);
					this.pubsubService.send(newInput.getSink(), newInput
							.getBytes());
				}

				inputList = collector.getBCastReduceInputs();
				for (ReduceInput input : inputList) {
					// System.out.println("BCasting :"+mapTaskNo+
					// "  to "+input.getSink());
					ReduceInput newInput = copyDataToCacheIfLargeAndGetReduceInput(
							input, jobConf.getSqrtReducers());
					this.pubsubService.send(newInput.getSink(), newInput
							.getBytes());
				}
			}
			this.pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE + "/"
					+ jobConf.getJobId(), status.getBytes());

		} catch (Exception e) {
			e.printStackTrace();
			synchronized (this) {
				this.hasException = true;
				this.exception = e;
				logger.error(e);
			}

			TaskStatus status = new TaskStatus(TwisterConstants.MAP_TASK,
					TwisterConstants.FAILED, mapTaskNo, (System
							.currentTimeMillis() - beginTime), iteration);
			status.setExceptionString(e.getMessage());
			try {
				this.pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE
						+ "/" + jobConf.getJobId(), status.getBytes());
			} catch (Exception e1) {
				logger.error(e);
			}
		}
	}

	private ReduceInput copyDataToCacheIfLargeAndGetReduceInput(
			ReduceInput input, int numReceivers) {
		try {
			byte[] inputData = input.getBytes();
			if (inputData.length < TwisterConstants.indirect_transfer_threashold) {
				return input;
			} else {
				String cacheKey = UUIDGenerator.getInstance()
						.generateTimeBasedUUID().toString();
				cgl.imr.types.StringValue tmpVal = new StringValue(hostIP + ":"
						+ daemonPort + ":" + cacheKey);
				this.dataCache.put(cacheKey, new DataHolder(input.getBytes(),
						numReceivers));
				input.getOutputs().clear();
				input.addKeyValue(TwisterConstants.fixed_key_M2R, tmpVal);
				input.setNoHasData();
			}
		} catch (SerializationException e) {
			e.printStackTrace();
		}

		return input;
	}

	public void setCurrentRequest(MapTaskRequest currentRequest) {
		this.currentRequest = currentRequest;
	}
}
