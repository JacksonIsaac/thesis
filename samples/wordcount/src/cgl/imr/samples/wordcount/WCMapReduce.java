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

package cgl.imr.samples.wordcount;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.Map;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.TwisterException;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.base.TwisterMonitor;

/**
 * Count the number of occurrences of words of a set of documents using
 * MapReduce.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class WCMapReduce {

	public static String DATA_FILE = "DATA_FILE";
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.out.println("Usage:[partition File][output file][num maps][num reducers]");
			System.exit(-1);
		}

		String partitionFile = args[0];
		String outputFile = args[1];
		int numMaps = Integer.parseInt(args[2]);
		int numReducers = Integer.parseInt(args[3]);
		
		WCMapReduce wc = new WCMapReduce();

		double beginTime = System.currentTimeMillis();
		Map<String, Integer> result = wc.wordCountMapReduce(partitionFile,
				outputFile, numMaps, numReducers);
		double endTime = System.currentTimeMillis();
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile,true));
		//writer.write(System.getProperty("line.separator"));
		//writer.flush();
		//writer.close();

		String word = null;
		int count = 0;
		for (Iterator<String> ite = result.keySet().iterator(); ite.hasNext();) {
			word = ite.next();
			if (count%100==99)
				System.out.println(word + " , " + result.get(word));
			writer.write(word + " , " + result.get(word)+"\n");
			count++;
			
		}
		writer.flush();
		writer.close();

		System.out.println("------------------------------------------------------");
		System.out.println("Word Count took " + (endTime - beginTime) / 1000
				+ " seconds.");
		System.out.println("------------------------------------------------------");
		System.exit(0);
	}

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	private Map<String, Integer> wordCountMapReduce(String partitionFile,
			String outputFile, int numMapTasks, int numReduceTasks)
			throws TwisterException {

		// JobConfigurations
		JobConf jobConf = new JobConf("word-count-map-reduce"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(WCMapTask.class);
		jobConf.setReducerClass(WCReduceTask.class);
		jobConf.setCombinerClass(WCCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		//jobConf.setFaultTolerance();

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);
		TwisterMonitor monitor = driver.runMapReduce();
		monitor.monitorTillCompletion();
		Map<String, Integer> wordCounts = ((WCCombiner) driver.getCurrentCombiner()).getResults();

		driver.close();
		return wordCounts;
	}

}
