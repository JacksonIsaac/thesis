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

package cgl.imr.samples.blastutil;

import java.util.Properties;

import cgl.imr.config.ConfigurationException;
import cgl.imr.util.PropertyLoader;

/**
 * load config file, specify BLAST execution command
 * And input option name and output option name
 * then the program can be adaptable to both BLAST+ and BLAST
 * 
 * 
 * @author Bingjing Zhang (zhangbj@cs.indiana.edu)
 * 		5/25/2010
 *
 */
public class Config {
	final static String key_execmd = "execmd";
	final static String key_inop = "inop";
	final static String key_outop = "outop";
	
	final static String properties_file = "twister_blast.properties";

	protected String exeCmd;
	protected String inOp;
	protected String outOp;

	public Config() throws ConfigurationException {
		this(properties_file);
	}

	protected Config(String propertiesFile)
			throws ConfigurationException {
		try {
			Properties properties = PropertyLoader
					.loadProperties(propertiesFile);
			exeCmd = properties.getProperty(key_execmd);
			inOp = properties.getProperty(key_inop);
			outOp = properties.getProperty(key_outop);

		} catch (Exception e) {
			throw new ConfigurationException(
					"Could not load Twister-BLAST propeties correctly.", e);
		}
	}

	public String getExeCmd() {
		System.out.println(exeCmd);
		return exeCmd;
	}
	
	public String getInOp() {
		System.out.println(inOp);
		return inOp;
	}
	
	public String getOutOp() {
		System.out.println(outOp);
		return outOp;
	}
}
