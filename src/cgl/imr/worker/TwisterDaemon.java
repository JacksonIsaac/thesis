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

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import cgl.imr.config.TwisterConfigurations;

/**
 * TwisterDaemon that is responsible for all the server side processing in
 * Twister framework. Once started, every daemon starts listening to a given
 * port which can be used to stop the daemons from running at the end of the
 * MapReduce computations or abruptly in the middle of a computation. This
 * method has no relation to the pub-sub broker communications and hence can be
 * used even in the case of pub-sub broker failures.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class TwisterDaemon {

	static int counter = 0;
	private static Logger logger = Logger.getLogger(TwisterDaemon.class);

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out
					.println("Usage: cgl.mr.worker.TwisterDaemon [Daemon No][Number Workers Threads = number of CPU cores (typically)][host]");
			return;
		}

		int daemonNo = Integer.valueOf(args[0]).intValue();
		int numWorkerThreads = Integer.valueOf(args[1]).intValue();

		TwisterDaemon daemon = new TwisterDaemon(daemonNo, numWorkerThreads,args[2]);
		daemon.run();
	}

	private int daemonNo = 0;
	private int daemonPortBase = 0;
	private int daemonPort;

	private DaemonWorker daemonWorker;
	private Executor taskExecutor = null;
	private ConcurrentHashMap<String, DataHolder> dataCache;

	public TwisterDaemon(int daemonNo, int numMapWorkers,String host) {
		super();

		taskExecutor = Executors.newCachedThreadPool();
		dataCache = new ConcurrentHashMap<String, DataHolder>();
		this.daemonNo = daemonNo;
		try {
			TwisterConfigurations configs = TwisterConfigurations.getInstance();
			this.daemonPortBase = configs.getDaemonPortBase();
			daemonPort = daemonPortBase + daemonNo;
			this.daemonWorker = new DaemonWorker(daemonNo, numMapWorkers,
					dataCache, daemonPort, host);

		} catch (Exception e) {
			logger.error("TwisterDaemon No" + daemonNo
					+ "failed to initialize due to error.", e);
			System.exit(-1);
		}
	}

	/**
	 * Handles the socket based communication, which is use to stop the
	 * TwisterDaemon.
	 */
	public void run() {
		ServerSocket serverSock = null;
		Socket sock = null;
		try {
			serverSock = new ServerSocket(daemonPort);
			while (true) {
				if (serverSock != null)
					sock = serverSock.accept();
				if (sock == null)
					continue;

				 DataSender sender = new DataSender(sock,dataCache, this.daemonWorker);
				 taskExecutor.execute(sender);

//				BufferedReader sockReader = new BufferedReader(
//						new InputStreamReader(sock.getInputStream()));
//				String cmd = null;
//				if ((cmd = sockReader.readLine()) != null) {
//					if (cmd.equals("quit")) {
//						// this.daemonWorker.termintate();
//						// break;
//						System.exit(0);
//					} else {
//						DataOutputStream dout = new DataOutputStream(sock
//								.getOutputStream());
//						DataHolder holder = dataCache.get(cmd);
//						if (holder != null) {
//							byte[] data = holder.getData();
//
//							// DataOutputStream dout = new
//							// DataOutputStream(sock.getOutputStream());
//							dout.write(data, 0, data.length);
//							// System.out.println("Wrting data #################### "+data.length);
//							dout.flush();
//							dout.close();
//							holder.decrementDownloadCount();
//							if (holder.getDowloadCount() <= 0) {
//								dataCache.remove(cmd);
//							}
//						}
//					}
//					sockReader.close();
//				}

			}
			// sock.close();
		} catch (Exception exp) {
			logger.error("TwisterDaemon No" + daemonNo
					+ "quiting due to error.", exp);
			System.exit(-1);
		}
		logger.info("TwisterDaemon No " + daemonNo + " quitting gracefully.");
		System.exit(0);
	}
}
