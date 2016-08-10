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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.Key;
import cgl.imr.base.PubSubService;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.ReduceOutputCollectorImpl;
import cgl.imr.message.CombineInput;
import cgl.imr.message.ReduceInput;
import cgl.imr.message.ReducerRequest;
import cgl.imr.message.TaskStatus;
import cgl.imr.types.IntKey;
import cgl.imr.types.StringKey;
import cgl.imr.types.StringValue;
import cgl.imr.util.CustomClassLoader;

/**
 * Executor for reduce tasks.Reducer holds the map outputs assigned to it until
 * all the outputs are received. Then it executes the reduce task.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class Reducer implements Runnable {

	private static Logger logger = Logger.getLogger(Reducer.class);	

	private String combineSink;
	private int numMapTasks;
	private int numReduceInputsReceived = 0;
	private PubSubService pubsubService;
	private Map<Key, List<Value>> reduceInputs;
	private ReduceTask reduceTask;
	private ReducerRequest request;
	private int reducerNo;
	private int iteration;
	ConcurrentHashMap<String, DataHolder> dataCache;
	int daemonPort;
	String hostIP;

	public Reducer(PubSubService pubsubService, ReducerRequest request,
			CustomClassLoader classLoader,ConcurrentHashMap<String, DataHolder> dataCache,int daemonPort,String hostIP) throws TwisterException {
		this.dataCache=dataCache;
		this.daemonPort=daemonPort;
		this.hostIP=hostIP;
		reduceInputs = new ConcurrentHashMap<Key, List<Value>>();
		this.pubsubService = pubsubService;
		this.request = request;
		this.iteration = request.getIteration();
		this.reducerNo = request.getReduceConf().getReduceTaskNo();

		this.combineSink = request.getCombineTopic();
		this.numMapTasks = request.getJobConf().getNumMapTasks();
		Class<?> c;
		try {
			c = classLoader.loadClass(request.getJobConf().getReduceClass());
			reduceTask = (ReduceTask) c.newInstance();
			reduceTask.configure(request.getJobConf(), request.getReduceConf());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
	}
	
	

	public int getReducerNo() {
		return reducerNo;
	}

	private void addKeyValueToReduceInputs(Key key, Value val) {
		List<Value> values = this.reduceInputs.get(key);
		if (values == null) {
			values = new ArrayList<Value>();
			values.add(val);
			this.reduceInputs.put(key, values);
		} else {
			values.add(val);
		}
	}
	
	
	private byte[] getDataFromServer(String host, int port, String key) throws TwisterException
	{
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
				wr.write(key+"\n");
				wr.flush();

				DataInputStream br = new DataInputStream(sock.getInputStream());
				byte[] buff = new byte[1048576];
				int len = 0;
																		
				while ((len = br.read(buff)) > 0) {
					bout.write(buff, 0, len);
					//System.out.println("Reading bytes -------------- "+len);
				}
				wr.close();
				br.close();

				//System.out.println("Finished reading. recevied " + bout.size());
				sock.close();
				//Thread.sleep(100);
				bout.flush();

		} catch (Exception e) {
			throw new TwisterException("Error in downloading reduce input",e);
		}
		return bout.toByteArray();
	}
	
	private ReduceInput getReduceInputFromRemoteHost(ReduceInput reduceInputTmp) throws NumberFormatException, TwisterException, SerializationException
	{
		//Map<Key,List<Value>> tmpMap=reduceInputTmp.getOutputs();
		StringValue memKey=null;//(StringValue)(tmpMap.get("known_key").get(0));//There is only one value here.
		
		Map<Key, List<Value>> tmpMap = reduceInputTmp.getOutputs();
		Iterator<Key> ite = tmpMap.keySet().iterator();
		Key key = null;
		List<Value> listOfValues = null;

		while (ite.hasNext()) {
			key = ite.next();
			listOfValues = tmpMap.get(key);
			// TODO Optimize this
			for (Value val : listOfValues) {
				memKey=(StringValue)val;
			}
		}
		
		String[] parts= memKey.toString().split(":");
		//System.out.println(memKey);
		byte[] data=null;		
		if(parts[0].trim().equals(hostIP))
		{
			DataHolder holder=dataCache.get(parts[2]);
			if(holder!=null)
			{
				data=holder.getData();	
				//ZBJ adds code here. Remove the global reference of the data.
				//In order to let gc collect the memory, otherwise there will be
				//memory leak.
				holder.decrementDownloadCount();
				if(holder.getDowloadCount()<=0)
				{
					dataCache.remove(parts[2]);
				}			
			}else
			{
				data=getDataFromServer(parts[0],Integer.parseInt(parts[1]),parts[2]);
			}
		}else{		
			data=getDataFromServer(parts[0],Integer.parseInt(parts[1]),parts[2]);
		}
		return new ReduceInput(data);
		
	}
	

	/**
	 * Adds the reduce inputs to the reduceinputs.
	 * 
	 * @param reduceInput
	 * @throws TwisterException 
	 * @throws SerializationException 
	 */
	public void handleReduceInputMessage(ReduceInput reduceInput) throws TwisterException, SerializationException {
			
		//double begin=System.currentTimeMillis();
		if(!reduceInput.isHasData()){
			 reduceInput	=getReduceInputFromRemoteHost(reduceInput);
		}
		
		//double end=System.currentTimeMillis();
		//System.out.println("Downlaod time ="+(end-begin)/1000+ " seconds.");
		
		// System.out.println("REDUCER recievied messages. @ "+System.currentTimeMillis());
		if (iteration != reduceInput.getIteration()) {
			System.out.println("Duplicate at the reducer.. iteration= "
					+ iteration + " inputs =" + reduceInput.getIteration());
			return; // This could be from a duplicate map task that could have
			// stuck in the past. We can ignore it.
		}

		Map<Key, List<Value>> tmpMap = reduceInput.getOutputs();
		Iterator<Key> ite = tmpMap.keySet().iterator();
		Key key = null;
		List<Value> listOfValues = null;

		while (ite.hasNext()) {
			key = ite.next();
			listOfValues = tmpMap.get(key);
			// TODO Optimize this
			for (Value val : listOfValues) {
				addKeyValueToReduceInputs(key, val);
			}
		}

		assert (reduceInputs.keySet().size() <= numMapTasks);
		synchronized (this) {
			numReduceInputsReceived++;
		}
	}
	
	
	
	public void handleReduceInputMessageForBcast(ReduceInput reduceInputTmp,Key newKey) throws NumberFormatException, TwisterException, SerializationException {
		// System.out.println("REDUCER recievied messages. @ "+System.currentTimeMillis());
		ReduceInput reduceInput	=getReduceInputFromRemoteHost(reduceInputTmp);
		
		if (iteration != reduceInput.getIteration()) {
			System.out.println("Duplicate at the reducer.. iteration= "
					+ iteration + " inputs =" + reduceInput.getIteration());
			return; // This could be from a duplicate map task that could have
			// stuck in the past. We can ignore it.
		}

		Map<Key, List<Value>> tmpMap = reduceInput.getOutputs();
		Iterator<Key> ite = tmpMap.keySet().iterator();
		Key key = null;
		List<Value> listOfValues = null;

		while (ite.hasNext()) {
			key = ite.next();
			listOfValues = tmpMap.get(key);
			// TODO Optimize this
			for (Value val : listOfValues) {
				addKeyValueToReduceInputs(newKey, val);
			}
		}

		assert (reduceInputs.keySet().size() <= numMapTasks);
		synchronized (this) {
			numReduceInputsReceived++;
		}
	}
	

	/**
	 * Check if all the reduce inputs have received.
	 * 
	 * @return true if all reduce inputs received, false otherwise.
	 */
//	public synchronized boolean isAllReduceInputsReceived() {
//		return (numReduceInputsReceived == numMapTasks) ? true : false;
//	}
	
	public int getNumReduceInputsReceived(){
		return numReduceInputsReceived; 
	}

	public void run() {
		ReduceOutputCollector collector = new ReduceOutputCollectorImpl(
				combineSink, iteration, reducerNo);
		long beginTime = 0;
		try {
			Iterator<Key> ite = reduceInputs.keySet().iterator();
			Key key = null;
			beginTime = System.currentTimeMillis();
			while (ite.hasNext()) {
				key = ite.next();
				// System.out.println("EXECUTING REDUCER %%%%%%%%%%%%%%%%= ");
				reduceTask.reduce(collector, key, reduceInputs.get(key));
			}
			long endTime = System.currentTimeMillis();
			TaskStatus status = new TaskStatus(TwisterConstants.REDUCE_TASK,
					TwisterConstants.SUCCESS, request.getReduceConf()
							.getReduceTaskNo(), (endTime - beginTime),
					iteration);

			CombineInput combineInput = collector.getCombineInput();
			/**
			 * Clean the maps and tables first for this reducer.
			 * 
			 */
			// System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&& INCERMENTING ITERATION = "+iteration);
			iteration++;
			reduceInputs.clear();
			
			synchronized (this) {
				numReduceInputsReceived = 0;
			}
			//There should be only combiner.
			CombineInput newInput=copyDataToCacheIfLargeAndGetReduceInput(combineInput,1);
			//pubsubService.send(combineInput.getCombineTopic(), newInput	.getBytes());
			pubsubService.send(newInput.getCombineTopic(), newInput	.getBytes());
			pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE + "/"
					+ request.getJobConf().getJobId(), status.getBytes());

		} catch (Exception e) {
			e.printStackTrace();
			TaskStatus status = new TaskStatus(TwisterConstants.REDUCE_TASK,
					TwisterConstants.FAILED, request.getReduceConf()
							.getReduceTaskNo(),
					(System.currentTimeMillis() - beginTime), iteration);
			logger.error(e);
			try {
				pubsubService.send(TwisterConstants.RESPONSE_TOPIC_BASE + "/"
						+ request.getJobConf().getJobId(), status.getBytes());
			} catch (Exception ex) {
				logger
						.error(
								"Error in reducer, could not send the exception to the client.",
								ex);
			}
		}
		//logger.debug("Reduce Task :"
		//		+ request.getReduceConf().getReduceTaskNo() + " terminating.");
		// System.out.println("REDUCER finished running. @ "+System.currentTimeMillis());
	}
	
//	private CombineInput copyDataToCacheIfLargeAndGetCombineInput(CombineInput input,int numReceivers){
//		
//		StringKey tmpKey=new StringKey("known_key");
//		String cacheKey=UUIDGenerator.getInstance().generateTimeBasedUUID().toString();
//		
//		try {		
//			cgl.imr.types.StringValue tmpVal= new StringValue(hostIP+":"+daemonPort+":"+cacheKey);
//			this.dataCache.put(cacheKey, new DataHolder(input.getBytes(), numReceivers) );
//			input.getOutputs().clear();
//			input.addKeyValue(tmpKey, tmpVal);
//		} catch (SerializationException e) {			
//			e.printStackTrace();
//		} 		
//		return input;		
//	}
	
	private CombineInput copyDataToCacheIfLargeAndGetReduceInput(
			CombineInput input, int numReceivers) {
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
				input.addKeyValue(TwisterConstants.fixed_key_R2C, tmpVal);
				input.setNoHasData();
			}
		} catch (SerializationException e) {
			e.printStackTrace();
		}

		return input;
	}

	public void terminate() throws TwisterException {
		this.reduceTask.close();
		this.reduceInputs.clear();
	}	
}
