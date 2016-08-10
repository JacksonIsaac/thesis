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

package cgl.imr.script;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Assume you have a node list in "nodes" file
 * 
 * Several things:
 * 
 * set amq.properties and nb.properties set Twister properties remove the node
 * for broker from the node list, reset nodes setup "stimr.sh", the memory for
 * each daemon, assuming all the nodes have the same amount of memory
 * 
 * Basically, the process for automatic setup has the following steps 1. read
 * node files, consider the special characteristics of Quarry system, we need to
 * generate a real node list 2. pickup one node as AMQ broker, for simplicity
 * currently, we can abstract these parts for adding multiple brokers and other
 * brokers such as NB in future. 3. then remove the node from the node list 4.
 * Set Twister properties, app_dir, nodes_file, data_dir, here data_dir could
 * change (especially on Quarry), daemons_per_node is usually 1, we need to
 * check the total number of cores. 5. Set the memory in stimr.sh, check the
 * number of memory size, set to half of the total 6. If not on quarry, you need
 * to use configure.sh to copy settings to all nodes
 */

class MultiNodeSetup {

	protected List<String> brokerAddresses;
	protected List<String> nodes;

	MultiNodeSetup() {
		brokerAddresses = new ArrayList<String>();
		nodes = new ArrayList<String>();
	}

	MultiNodeSetup(List<String> nodelist) {
		brokerAddresses = new ArrayList<String>();
		nodes = nodelist;
	}

	void quickConfigure() {
		processNodes();
		selectBrokers();
		setupBrokers();
		setupNodes();
		setupTwisterProperties();
		setupStImr();
		replicateConfiguration();
		System.out.println("Auto configuration is done. ");
	}

	protected void processNodes() {
		// special processing to node lines got from the file
		System.out.println("no special processing to nodes");
	}

	private void selectBrokers() {
		this.brokerAddresses.add(this.nodes.get(this.nodes.size() - 1));
	}

	private void setupBrokers() {
		setupAMQ();
	}

	private void setupAMQ() {
		Properties properties = new Properties();
		try {
			properties.load(new FileReader(QuickDeployment.amq_properties));

			String value = "failover:(tcp://" + this.brokerAddresses.get(0)
					+ ":61616)";
			properties.setProperty(QuickDeployment.key_uri, value);

			System.out.println("ActiveMQ uri="
					+ properties.getProperty(QuickDeployment.key_uri));

			String comments = readComments(QuickDeployment.amq_properties);

			properties.store(new FileWriter(QuickDeployment.amq_properties),
					comments);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String readComments(String properties_filename) {
		StringBuffer comments = new StringBuffer();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					properties_filename));

			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("#")) {
					comments.append(line.replace("#", "") + "\n");
				}
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// System.out.println(comments);
		return new String(comments);
	}

	private void setupNodes() {
		// delete the original file
		File nodeFile = new File(QuickDeployment.nodes_file);
		if (nodeFile.exists()) {
			nodeFile.delete();
		}

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					QuickDeployment.nodes_file));
			for (int i = 0; i < this.nodes.size(); i++) {
				writer.write(this.nodes.get(i));
				writer.newLine();
			}
			writer.flush();
			writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setupTwisterProperties() {

		Properties properties = new Properties();
		try {
			String twisterHome = QuickDeployment.getTwisterHome();

			properties.load(new FileReader(QuickDeployment.twister_properties));

			// set nodes file location
			String nodes_file_value = twisterHome + "bin/nodes";
			properties.setProperty(QuickDeployment.key_nodes_file,
					nodes_file_value);
			System.out.println("nodes_file="
					+ properties.getProperty(QuickDeployment.key_nodes_file));

			// set daemons per node
			properties.setProperty(QuickDeployment.key_daemons_per_node, "1");
			System.out.println("daemons_per_node="
					+ properties
							.getProperty(QuickDeployment.key_daemons_per_node));

			// set workers per node
			properties.setProperty(QuickDeployment.key_workers_per_daemon,
					Runtime.getRuntime().availableProcessors() + "");
			System.out
					.println("workers_per_daemon="
							+ properties
									.getProperty(QuickDeployment.key_workers_per_daemon));

			// set app dir
			String app_dir_value = twisterHome + "apps";
			properties.setProperty(QuickDeployment.key_app_dir, app_dir_value);
			System.out.println("app_dir="
					+ properties.getProperty(QuickDeployment.key_app_dir));

			// set data_dir
			properties.setProperty(QuickDeployment.key_data_dir,
					createDataDir());
			System.out.println("data_dir="
					+ properties.getProperty(QuickDeployment.key_data_dir));

			String comments = readComments(QuickDeployment.twister_properties);

			properties.store(
					new FileWriter(QuickDeployment.twister_properties),
					comments);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected String createDataDir() {
		String twisterHome = QuickDeployment.getTwisterHome();
		String data_dir = twisterHome + "data";
		QuickDeployment.createDir(data_dir);
		return data_dir;
	}

	private void setupStImr() {
		StringBuffer contents = new StringBuffer();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(
					QuickDeployment.stimr_sh));

			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("java")) {
					//int min_mem = QuickDeployment.getTotalMem()  / 3;
					int max_mem = QuickDeployment.getTotalMem()  * 2 / 3;
					//line = line.replaceFirst("-Xms[0-9]*m", "-Xms" + min_mem + "m");
					//System.out.println("Change initial memory to " + min_mem + " MB");
					line = line.replaceFirst("-Xmx[0-9]*m", "-Xmx" + max_mem + "m");
					System.out.println("Change max memory to " + max_mem + " MB");
					
				}
				contents.append(line + "\n");
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// delete the original file
		File stimrFile = new File(QuickDeployment.stimr_sh);
		if (stimrFile.exists()) {
			stimrFile.delete();
		}

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					QuickDeployment.stimr_sh));
			writer.write(new String(contents));
			writer.flush();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		File newstimrFile = new File(QuickDeployment.stimr_sh);
		newstimrFile.setExecutable(true);
	}

	protected void replicateConfiguration() {
		QuickDeployment.doConfigureSh();
	}
}
