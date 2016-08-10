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

package cgl.imr.samples.pagerank;

import java.util.ArrayList;
import java.util.List;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.base.TwisterMonitor;
//import cgl.imr.monitor.TwisterMonitor;
import cgl.imr.types.IntKey;
import cgl.imr.types.StringValue;

/**
 * Generate adjacency matrix for urls using MapReduce. It uses a "map-only"
 * operation to generate data concurrently.
 * 
 * @author Hui Li (lihui@indiana.edu)
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 */

public class PageRankDataGen {

	public static String DATA_FILE_SUFFIX = ".txt";
	public static String PROP_DATA_DIR = "prop_data_dir";
	public static String PROP_NUM_MAP_TASKS = "prop_num_map_tasks";
	public static String PROP_NUM_URLS = "prop_num_urls";
	
	
	
	/**
	 * Produces a list of key,value pairs for map tasks.
	 * 
	 * @param numMaps
	 *            - number of map tasks.
	 * @param numUrls
	 *            - number of urls in the job
	 * @param dataFilePrefix
	 *            - the pattern of input file name
	 * 
	 * @return - List of key,value pairs.
	 */

	private static List<KeyValuePair> getKeyValuesForMap(int numMaps,
			long numUrls, String dataFilePrefix) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		IntKey key = null;
		StringValue value = null;
		for (int i = 0; i < numMaps; i++) {
			key = new IntKey(i);
			value = new StringValue(dataFilePrefix + i + DATA_FILE_SUFFIX);
			keyValues.add(new KeyValuePair(key, value));
		}
		return keyValues;
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			String errorReport = "PageRankDataGen: The Correct arguments are \n"
					+ "java cgl.mr.pagerank.PageRankDataGen "
					+ "[data dir][data file prefix][num splits=num map tasks][num urls]";
			System.out.println(errorReport);
			System.exit(-1);
		}
		String dataDir = args[0]; // Data directory inside the data_dir. 
		String dataFilePrefix = args[1];
		int numMapTasks = Integer.parseInt(args[2]);
		long numUrls = Long.parseLong(args[3]);
		PageRankDataGen client;
		try {
			client = new PageRankDataGen();
			client.driveMapReduce(numMapTasks, numUrls, dataDir, dataFilePrefix);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}

	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	public void driveMapReduce(int numMapTasks, long numUrls, String dataDir,
			String dataFilePrefix) throws Exception {

		int numReducers = 0; // We don't need any reducers.
		// JobConfigurations

		JobConf jobConf = new JobConf("pagerank-data-gen"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(PageRankDataGenMapTask.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReducers);
		jobConf.addProperty(PROP_NUM_URLS, String.valueOf(numUrls));
		jobConf.addProperty(PROP_NUM_MAP_TASKS, String.valueOf(numMapTasks));
		jobConf.addProperty(PROP_DATA_DIR, dataDir);

		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps();
		
		TwisterMonitor monitor = driver.runMapReduce(getKeyValuesForMap(
				numMapTasks, numUrls, dataFilePrefix));
		monitor.monitorTillCompletion();
		driver.close();
	}
}
