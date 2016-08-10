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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Select suitable setup mode, and execute setup
 * 
 * @author zhangbj
 * 
 */
public class QuickDeployment {

	final static String nodes_file = "nodes";
	final static String amq_properties = "amq.properties";
	final static String key_uri = "uri";
	final static String twister_properties = "twister.properties";
	final static String key_nodes_file = "nodes_file";
	final static String key_daemons_per_node = "daemons_per_node";
	final static String key_workers_per_daemon = "workers_per_daemon";
	final static String key_app_dir = "app_dir";
	final static String key_data_dir = "data_dir";
	final static String stimr_sh = "stimr.sh";

	static MultiNodeSetup getSetupInstance() {
		List<String> nodes = getNodeAdresses();
		if (nodes.size() <= 1) {
			System.out.println("use StandAlone Setup.");
			return new StandAloneSetup(nodes);
		} else if (PGSetup.isPG(nodes)) {
			System.out.println("use PG Setup");
			return new PGSetup(nodes);
		}

		System.out.println("use normal MultiNode Setup");
		return new MultiNodeSetup(nodes);
	}

	/**
	 * read "nodes" file under the current directory, get the nodes set
	 */
	private static List<String> getNodeAdresses() {
		Set<String> nodes = new HashSet<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(nodes_file));
		} catch (FileNotFoundException e) {
			System.out.println("No node file found. ");
			// e.printStackTrace();
		}

		if (reader == null) {
			nodes.add("127.0.0.1");
			return new ArrayList<String>(nodes);
		}

		String line = "";
		try {
			while ((line = reader.readLine()) != null) {
				nodes.add(line.trim());
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (nodes.size() == 0) {
			nodes.add("127.0.0.1");
		}

		return new ArrayList<String>(nodes);
	}

	private static List<String> exeuteCMD(String[] cmd) {
		List<String> output = new ArrayList<String>();

		try {
			Process q = Runtime.getRuntime().exec(cmd);
			q.waitFor();
			InputStream is = q.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);

			String line;
			while ((line = br.readLine()) != null) {
				output.add(line);
				// System.out.println(line);
			}
			br.close();
			isr.close();
			is.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output;
	}

	static String getPWD() {
		String cmdstr[] = { "pwd" };
		List<String> output = QuickDeployment.exeuteCMD(cmdstr);
		return output.get(0).replace(" ", "\\ ");
	}

	static String getTwisterHome() {
		String cmdstr[] = { "bash", "-c", "echo $TWISTER_HOME" };
		List<String> output = QuickDeployment.exeuteCMD(cmdstr);
		String twister_home = output.get(0) + "/";
		twister_home = twister_home.replace("//", "/");
		return twister_home.replace(" ", "\\ ");
	}

	static int getTotalMem() {
		String mem = "0";
		String cmdstr[] = { "cat", "/proc/meminfo" };
		List<String> output = QuickDeployment.exeuteCMD(cmdstr);

		for (int i = 0; i < output.size(); i++) {
			if (output.get(i).contains("MemTotal:")) {
				mem = output.get(i).replace("MemTotal:", "").replace("kB", "")
						.trim();
				break;
			}
		}

		double memory = Double.parseDouble(mem);
		int memMB = (int) (memory / (double) 1024);

		return memMB;
	}

	static void createDir(String path) {
		String twisterSh = getTwisterHome() + "bin/twister.sh";
		File twisterShFile = new File(twisterSh);
		twisterShFile.setExecutable(true);
		// String cmdstr1[] = { "chmod", "a+x", twisterSh };
		// QuickDeployment.exeuteCMD(cmdstr1);
		String cmdstr[] = { twisterSh, "initdir", path };
		List<String> output = QuickDeployment.exeuteCMD(cmdstr);
		for (int i = 0; i < output.size(); i++) {
			System.out.println(output.get(i));
		}
	}

	static String getUserName() {
		String cmdstr[] = { "whoami" };
		List<String> output = QuickDeployment.exeuteCMD(cmdstr);
		return output.get(0);
	}

	static void doConfigureSh() {
		String configureSh = getTwisterHome() + "bin/configure.sh";
		String cmdstr[] = { configureSh };
		List<String> output = QuickDeployment.exeuteCMD(cmdstr);
		for (int i = 0; i < output.size(); i++) {
			System.out.println(output.get(i));
		}
	}

	public static void main(String[] args) {
		MultiNodeSetup setup = QuickDeployment.getSetupInstance();
		setup.quickConfigure();
	}
}
