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

package cgl.imr.samples.blastdbextract;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;

/**
 * BLAST Database can be sent in tar package with a reduced size.
 * This is for uncompress these package parallel.
 * 
 * @author Bingjing Zhang (zhangbj@cs.indiana.edu)
 * 		5/25/2010
 *
 */
public class BLASTDBExtract {

	private static UUIDGenerator uuidGen = UUIDGenerator.getInstance();

	/**
	 * Launch MapReduceJob.
	 * The location of tar package on different node is
	 * configured by partition.pf
	 * 
	 * @param numMapTasks
	 * @param numReduceTasks
	 * @param partitionFile
	 * @param dataDir
	 * @throws TwisterException
	 */
	public static void driveMapReduce(int numMapTasks, int numReduceTasks,
			String partitionFile, String dataDir) throws TwisterException {

		// JobConfigurations
		JobConf jobConf = new JobConf("BLASTDBUncompress"
				+ uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(BLASTDBExtractMapTask.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		jobConf.addProperty("dataDir", dataDir);

		TwisterModel mrDriver = null;
		TwisterMonitor monitor = null;
		try {
			mrDriver = new TwisterDriver(jobConf);
			mrDriver.configureMaps(partitionFile);

			// System.out.println("START MAP REDUCE");
			monitor = mrDriver.runMapReduce();
			monitor.monitorTillCompletion();
			mrDriver.close();

		} catch (Exception e) {
			mrDriver.close();
			throw new TwisterException(e);
		}
	}
	
	/**
	 * main function
	 * the number of Map tasks = number_of_Files_per_node * num_of_nodes
	 * output_dir is a directory relative to TWISTER_DATA_HOME
	 * 
	 * @param args
	 */
	public static void main(String args[]) {

		if (args.length != 3) {
			System.err
					.println("args:  [num_map_tasks] [partition_file] [output_data_dir]");
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
		String partitionFile = args[1];
		String dataDir = (tconf.getLocalDataDir() + "/" + args[2] + "/").replace(
				"//", "/");
		
		long beforeTime = System.currentTimeMillis();

		try {
			driveMapReduce(numMapTasks, numReduceTasks, partitionFile, dataDir);
		} catch (TwisterException e) {
			e.printStackTrace();
		}

		double timeInSeconds = ((double) (System.currentTimeMillis() - beforeTime)) / 1000;
		System.out.println("Total Time of Extracting BLASTDB : "
				+ timeInSeconds + "Seconds");
		System.exit(0);
	}
}
