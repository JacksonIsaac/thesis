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

package cgl.imr.samples.blast;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.samples.blastutil.Config;
import cgl.imr.samples.blastutil.Query;
import cgl.imr.types.BytesValue;
import cgl.imr.types.StringKey;

/**
 * main class for Twister-BLAST.
 * Read the whole query file line by line.
 * Then separate them into groups
 * Each group has equal sized query lines and is
 * sent as key-value pairs and handled by 
 * map tasks.
 * 
 * @author Bingjing Zhang (zhangbj@cs.indiana.edu)
 * 		5/25/2010
 *
 */
public class BLAST {

	private static UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	/**
	 * Launch MapReduce job.
	 * 
	 * @param numMapTasks
	 * @param numReduceTasks
	 * @param noOfSequences
	 * @param queryFile
	 * @param dataDir
	 * @param inputPrefix
	 * @param outputPrefix
	 * @param exeCmd
	 * @param inOp
	 * @param outOp
	 * @throws TwisterException
	 */
	public static void driveMapReduce(int numMapTasks, int numReduceTasks,
			int noOfSequences, String queryFile, String dataDir,
			String inputPrefix, String outputPrefix, String exeCmd,
			String inOp, String outOp) throws TwisterException {

		List<Query> queries = null;
		try {
			queries = buildQueries(queryFile, noOfSequences, numMapTasks);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("No of Queries " + queries.size());

		//tests
		/*
		for(int i=0; i<queries.size(); i++) {
			Query query = (Query)queries.get(i);
			query.print();
			
			System.out.println();
		}
		System.exit(0);
		*/
		
		// JobConfigurations
		JobConf jobConf = new JobConf("TwisterBLAST"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(BLASTMapTask.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		jobConf.addProperty("dataDir", dataDir);
		jobConf.addProperty("inputPrefix", inputPrefix);
		jobConf.addProperty("outputPrefix", outputPrefix);
		jobConf.addProperty("exeCmd", exeCmd);
		jobConf.addProperty("inOp", inOp);
		jobConf.addProperty("outOp", outOp);
		//jobConf.setFaultTolerance();

		TwisterModel mrDriver = null;
		TwisterMonitor monitor = null;
		try {
			mrDriver = new TwisterDriver(jobConf);
			mrDriver.configureMaps();

			// System.out.println("START MAP REDUCE");
			monitor = mrDriver.runMapReduce(disseminateQueries(queries));
			monitor.monitorTillCompletion();
			mrDriver.close();
			
		} catch (Exception e) {
			mrDriver.close();
			throw new TwisterException(e);
		}
	}

	/**
	 * put query lines into query groups.  
	 * every query group has nearly the same size of query.
	 * If they can not be divided equally, the rest will be
	 * scattered into different group averagely.
	 * 
	 * @param queryFile
	 * @param noOfSequences
	 * @param numMapTasks
	 * @return
	 * @throws Exception
	 */
	private static List<Query> buildQueries(String queryFile,
			int noOfSequences, int numMapTasks) throws Exception {

		List<Query> queries = new ArrayList<Query>();

		int normalQuantity = noOfSequences / numMapTasks;
		int maxQuantity = normalQuantity + 1;
		int rest = noOfSequences - normalQuantity*numMapTasks;

		System.out.println("normalQuantity :" + normalQuantity);
		System.out.println("maxQuantity :" + maxQuantity);
		System.out.println("rest :" + rest);

		BufferedReader reader = new BufferedReader(new FileReader(queryFile));

		Query query = new Query();
		int count = 0;
		for (int i = 0; i < noOfSequences; i++) {
			String descriptionLine = null;
			String dataLine = null;

			if (((descriptionLine = reader.readLine()) == null)
					|| ((dataLine = reader.readLine()) == null)) {
				throw new IOException(
						"Cannot read the sequence from input file.");
			}

			//System.out.println(descriptionLine);
			//System.out.println(dataLine);
			
			query.addLine(descriptionLine);
			query.addLine(dataLine);
			count++;
			
			if (rest > 0) {
				if (count == maxQuantity) {
					// since one of the rest is added into the query, subtract
					// it
					rest--;
					queries.add(query);
					
					// if rest >0, it is not possible to be the last one
					count = 0;
					query = new Query();
				}
			} else {
				// rest == 0
				if (count == normalQuantity) {
					queries.add(query);
					if (i != noOfSequences - 1) {
						count = 0;
						query = new Query();
					}
				}
			}
		}

		reader.close();
		return queries;
	}

	/**
	 * transfer query groups into key value pairs
	 * 
	 * @param queries
	 * @return
	 * @throws Exception
	 */
	private static List<KeyValuePair> disseminateQueries(List<Query> queries)
			throws Exception {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();

		StringKey key = null;
		BytesValue value = null;

		int noOfQueries = queries.size();

		for (int i = 0; i < noOfQueries; i++) {
			key = new StringKey("" + i);
			value = new BytesValue(queries.get(i).getBytes());
			keyValues.add(new KeyValuePair(key, value));
		}
		return keyValues;
	}

	public static void main(String args[]) {
		
		if (args.length != 6) {
			System.err
					.println("args:  [num_map_tasks] [sequence_count] [query_file] [data_dir] [input_prefix] [output_prefix]");
			System.exit(2);
		}
		
		//load Twister Data Home directory
		TwisterConfigurations tconf = null;
		try {
			tconf = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}

		int numMapTasks = Integer.parseInt(args[0]);
		int numReduceTasks = 0;
		int noOfSequences = Integer.parseInt(args[1]);
		String queryFile = args[2];
		
		//working directory for putting temporary input files and final outputs
		//It is relative to Twister_DATA_HOME
		String dataDir = (tconf.getLocalDataDir() + "/" + args[3] + "/").replace(
				"//", "/");
		String inputPrefix = args[4];
		String outputPrefix = args[5];
		
		String exeCmd = null;
		String inOp = null;
		String outOp = null; 
		try {
			Config conf = new Config();
			exeCmd = conf.getExeCmd();
			inOp = conf.getInOp();
			outOp = conf.getOutOp();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		
		long beforeTime = System.currentTimeMillis();

		try {
			driveMapReduce(numMapTasks, numReduceTasks, noOfSequences,
					queryFile, dataDir, inputPrefix, outputPrefix, exeCmd, inOp, outOp);
		} catch (TwisterException e) {
			e.printStackTrace();
		}

		double timeInSeconds = ((double) (System.currentTimeMillis() - beforeTime)) / 1000;
		System.out.println("Total Time for BLAST : " + timeInSeconds
				+ "Seconds");
		System.exit(0);
	}
}
