/**
 * Software License, Version 1.0
 * 
 * Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 * 
 *
 *Redistribution and use in source and binary forms, with or without 
 *modification, are permitted provided that the following conditions are met:
 *
 *1) All redistributions of source code must retain the above copyright notice,
 * the list of authors in the original source code, this list of conditions and
 * the disclaimer listed in this license;
 *2) All redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the disclaimer listed in this license in
 * the documentation and/or other materials provided with the distribution;
 *3) Any documentation included with all redistributions must include the 
 * following acknowledgement:
 *
 *"This product includes software developed by the Community Grids Lab. For 
 * further information contact the Community Grids Lab at 
 * http://communitygrids.iu.edu/."
 *
 * Alternatively, this acknowledgement may appear in the software itself, and 
 * wherever such third-party acknowledgments normally appear.
 * 
 *4) The name Indiana University or Community Grids Lab or Twister, 
 * shall not be used to endorse or promote products derived from this software 
 * without prior written permission from Indiana University.  For written 
 * permission, please contact the Advanced Research and Technology Institute 
 * ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 *5) Products derived from this software may not be called Twister, 
 * nor may Indiana University or Community Grids Lab or Twister appear
 * in their name, without prior written permission of ARTI.
 * 
 *
 * Indiana University provides no reassurances that the source code provided 
 * does not infringe the patent or any other intellectual property rights of 
 * any other entity.  Indiana University disclaims any liability to any 
 * recipient for claims brought by any other entity based on infringement of 
 * intellectual property rights or otherwise.  
 *
 *LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO 
 *WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 *NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF 
 *INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS. 
 *INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS", 
 *"VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.  
 *LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR 
 *ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION 
 *GENERATED USING SOFTWARE.
 */

package cgl.imr.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.PubSubException;
import cgl.imr.base.PubSubService;
import cgl.imr.base.SerializationException;
import cgl.imr.base.Subscribable;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterConstants.EntityType;
import cgl.imr.base.impl.PubSubFactory;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.message.DaemonStatusMessage;
import cgl.imr.worker.StatusNotifier;

public class FaultDetector implements Subscribable {
	
	
	class FaultDetectorWorker extends Thread{	

		private boolean stop=false;
				
		public FaultDetectorWorker(){	
			//System.out.println("DETECTOR STARTED..");
		}
		
		public void stopWorker(){
			stop=true;
		}
		
		public void run(){			
			Integer daemonNo;
			DaemonStatus status;
			while(!stop){
				Iterator<Integer> ite =daemons.keySet().iterator();
				while(ite.hasNext()){
					daemonNo=ite.next();
					status=daemons.get(daemonNo);
					if( status.isRunning() && (System.currentTimeMillis()-status.getLastAliveTime())>TwisterConstants.MAX_WAIT_TIME_FOR_FAULT){
						synchronized (this) {
						System.out.println("FAILURE DETECTED ##################### Daemon " + daemonNo);
						status.setRunning(false);						
						lastFaultDetectedTime=System.currentTimeMillis();
						}
					}
						
				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	

	private static Logger logger = Logger.getLogger(FaultDetector.class);

	private ConcurrentHashMap<Integer, DaemonStatus> daemons = new ConcurrentHashMap<Integer, DaemonStatus>();
	private TwisterConfigurations config;
	//private int numNodes;
	//private int numDaemons;
	private PubSubService pubSubService;
	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();
	private boolean enabled = false;
	private long lastFaultDetectedTime;
	private long lastFaultServicedTime;
	private FaultDetectorWorker worker;
	
	// ZBJ: the reason to add this is to stop reporting fault during the stage of fault handling
	private boolean isFaultHandlerWorking = false; 

	public FaultDetector(Map<Integer, DaemonStatus> daemons,boolean enabled) throws ConfigurationException,
			IOException {
		this.daemons.putAll(daemons);
		config = TwisterConfigurations.getInstance();
		//numNodes = getNumNodes();
		//numDaemons = numNodes * config.getDamonsPerNode();
		//daemons = new ConcurrentHashMap<Integer, DaemonStatus>();
		//for (int i = 0; i < numDaemons; i++) {
			//Initially all daemons are assumed to be alive.
		//	daemons.put(i, new DaemonStatus(true, System.currentTimeMillis()));
		//}

		if (enabled) {
			this.enabled = true;
			int entityId = new Random(System.currentTimeMillis()).nextInt() * 2000000; // entity

			try {
				this.pubSubService = PubSubFactory.getPubSubService(config,
						EntityType.DRIVER, entityId);
				this.pubSubService.setSubscriber(this);
				this.pubSubService
						.subscribe(StatusNotifier.DAEMON_STATUS_TOPIC);
				worker=new FaultDetectorWorker();
				worker.start();
			} catch (PubSubException e) {
				if (this.pubSubService != null) {
					try {
						pubSubService.close();
					} catch (PubSubException e1) {
					}
					logger.error("Falied to initialize broker connection at the fault detector",e);
				}
			}
		}
	}
	
	public void updateDaemonStatus(Map<Integer, DaemonStatus> daemons){
		this.daemons.putAll(daemons);
	}
	

	public long getLastFaultDetectedTime() {
		return lastFaultDetectedTime;
	}

	public long getLastFaultServicedTime() {
		return lastFaultServicedTime;
	}

	public void setLastFaultServicedTime(long lastFaultServicedTime) {
		this.lastFaultServicedTime = lastFaultServicedTime;
	}

	public boolean isHasFault(){
		// ZBJ: stop reporting faults during fault handling
		return (((lastFaultServicedTime-lastFaultDetectedTime)>=0) || isFaultHandlerWorking ? false:true);
	}
	
	public void setFaultHandlerIsWorking(boolean isWorking) {
		isFaultHandlerWorking = isWorking;
	}


	protected int getNumNodes() throws IOException {
		int count = 0;
		BufferedReader reader;
		reader = new BufferedReader(new FileReader(config.getNodeFile()));
		while (reader.readLine() != null) {
			count++;
		}
		reader.close();
		return count;
	}

	public synchronized void close() throws TwisterException {
		if (enabled) {
			try {
				this.pubSubService.close();
				this.worker.stopWorker();
			} catch (PubSubException e) {
				throw new TwisterException(e);
			}
		}
	}

	public List<Integer> getAvailableDeamons() {
		List<Integer> availbleDaemons = new ArrayList<Integer>();
		Iterator<Integer> ite = daemons.keySet().iterator();
		Integer daemonNo;
		while (ite.hasNext()) {
			daemonNo = ite.next();
			if (daemons.get(daemonNo).isRunning()) {
				availbleDaemons.add(daemonNo);
			}
		}
		return availbleDaemons;
	}

	@Override
	public void onEvent(byte[] message) {
		if (message[0] == TwisterConstants.DAEMON_STATUS) {
			DaemonStatusMessage status;
			try {
				status = new DaemonStatusMessage(message);
				//System.out.println("Status :" + status.getDaemonNo() + " "
				//		+ status.getHostIP());
				DaemonStatus daemonStatus=daemons.get(status.getDaemonNo());				
				if(daemonStatus!=null){
					daemonStatus.setRunning(true);
					daemonStatus.setLastAliveTime(System.currentTimeMillis());
				}else{
					logger.error("Invalid daemon no. Inconsistant runtime state.");
				}
			} catch (SerializationException e) {
				logger.error(e);
			}

		}
	}
}
