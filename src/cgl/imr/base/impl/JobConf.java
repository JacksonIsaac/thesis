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

package cgl.imr.base.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;
import cgl.imr.base.TwisterSerializable;

/**
 * Configuration for MapReduce computations. Groups various properties needed by
 * the framework to execute a MapReduce computation.
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public class JobConf implements TwisterConstants, TwisterSerializable {

	private String combinerClass;
	private boolean hasCombinerClass = false;
	private boolean hasReduceClass = false;
	private String jobId;
	private String mapClass;
	private int numMapTasks;
	private int numReduceTasks;
	private Hashtable<String, String> properties;
	private String reduceClass;
	private boolean faultTolerance=false;
	private String reducerSelectorClass;
	
	private String rowBCastTopic;
	private boolean rowBCastSupported;
	private int sqrtReducers;
	
	private JobConf() {
		properties = new Hashtable<String, String>();
		// Setting the default reducer selector.
		this.reducerSelectorClass = HashBasedReducerSelector.class.getName();
	}

	public JobConf(byte[] bytes) throws SerializationException {
		this();
		this.fromBytes(bytes);
	}

	public JobConf(String jobId) {
		this.jobId = jobId;
		properties = new Hashtable<String, String>();
		// Setting the default reducer selector.
		this.reducerSelectorClass = HashBasedReducerSelector.class.getName();
	}

		
	public boolean isRowBCastSupported() {
		return rowBCastSupported;
	}

	public void setRowBCastSupported(boolean rowBCastSupported) {
		this.rowBCastSupported = rowBCastSupported;
		this.rowBCastTopic=MAP_TO_REDUCE_ROW_WISE_BCAST+jobId;	
		if(numReduceTasks==0){
			throw new RuntimeException("Please set the number of reduce tasks first.");
		}
		double sqrt=Math.sqrt(numReduceTasks);
		if((Math.ceil(sqrt)-sqrt)!=0){
			throw new RuntimeException("To use the row broadcast option the number of reduce tasks must have a perfect square.");
		}
		this.sqrtReducers=(int)sqrt;
	}

	public String getRowBCastTopic() {
		return rowBCastTopic;
	}

	public void addProperty(String key, String val) {
		this.properties.put(key, val);
	}	

	public boolean isFaultTolerance() {
		return faultTolerance;
	}

	public void setFaultTolerance() {
		this.faultTolerance = true;
	}
	
	

	public int getSqrtReducers() {
		return sqrtReducers;
	}

	/**
	 * Serializes the <code>JobConf</code> object.
	 */
	public void fromBytes(byte[] bytes) throws SerializationException {

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		int len = 0;
		byte[] data = null;
		try {
			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.jobId = new String(data);
			
			faultTolerance=din.readBoolean();

			numMapTasks = din.readInt();
			numReduceTasks = din.readInt();

			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.mapClass = new String(data);

			this.hasReduceClass = din.readBoolean();

			if (this.hasReduceClass) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.reduceClass = new String(data);
			}

			this.hasCombinerClass = din.readBoolean();
			if (this.hasCombinerClass) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.combinerClass = new String(data);
			}

			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.reducerSelectorClass = new String(data);
			
			this.rowBCastSupported = din.readBoolean();
			if (this.rowBCastSupported) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.rowBCastTopic = new String(data);
				this.sqrtReducers =din.readInt();
			}

			int numProperties = din.readInt();
			for (int i = 0; i < numProperties; i++) {

				int keyLen = din.readInt();
				byte[] keyBytes = new byte[keyLen];
				din.readFully(keyBytes);

				int valLen = din.readInt();
				byte[] valBytes = new byte[valLen];
				din.readFully(valBytes);

				this.properties.put(new String(keyBytes), new String(valBytes));
			}
			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	/**
	 * Deserializes the <code>JobConf</code> object.
	 */
	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;

		byte[] data = null;
		try {

			data = jobId.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			
			dout.writeBoolean(faultTolerance);

			dout.writeInt(numMapTasks);
			dout.writeInt(numReduceTasks);

			data = mapClass.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			dout.writeBoolean(this.hasReduceClass);
			if (this.hasReduceClass) {
				data = reduceClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);
			}

			dout.writeBoolean(this.hasCombinerClass);
			if (this.hasCombinerClass) {
				data = combinerClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);
			}

			data = reducerSelectorClass.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			
			dout.writeBoolean(this.rowBCastSupported);
			if (this.rowBCastSupported) {
				data = rowBCastTopic.getBytes();
				dout.writeInt(data.length);
				dout.write(data);
				dout.writeInt(sqrtReducers);
			}

			int numProperties = this.properties.size();
			dout.writeInt(numProperties);
			// Have to write key value pairs for properties
			Iterator<String> ite = properties.keySet().iterator();
			String key = null;
			while (ite.hasNext()) {
				key = ite.next();
				byte[] keyBytes = key.getBytes();
				dout.writeInt(keyBytes.length);
				dout.write(keyBytes);

				byte[] valBytes = properties.get(key).getBytes();
				dout.writeInt(valBytes.length);
				dout.write(valBytes);
			}

			dout.flush();
			marshalledBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}

		return marshalledBytes;
	}

	public String getCombinerClass() {
		return combinerClass;
	}

	public String getJobId() {
		return jobId;
	}

	public String getMapClass() {
		return mapClass;
	}

	public int getNumMapTasks() {
		return numMapTasks;
	}

	public int getNumReduceTasks() {
		return numReduceTasks;
	}

	public Hashtable<String, String> getProperties() {
		return this.properties;
	}

	public String getProperty(String key) {
		return this.properties.get(key);
	}

	public String getReduceClass() {
		return reduceClass;
	}

	public String getReducerSelectorClass() {
		return reducerSelectorClass;
	}

	public boolean isHasCombinerClass() {
		return hasCombinerClass;
	}

	public boolean isHasProperties() {
		return properties.size() > 0 ? true : false;
	}

	public boolean isHasReduceClass() {
		return hasReduceClass;
	}

	public void setCombinerClass(Class<?> combinerClass) {
		this.combinerClass = combinerClass.getName();
		this.hasCombinerClass = true;
	}

	public void setHasReduceClass(boolean hasReduceClass) {
		this.hasReduceClass = hasReduceClass;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public void setMapperClass(Class<?> mapClass) {
		this.mapClass = mapClass.getName();
	}

	public void setNumMapTasks(int numMapTasks) {
		this.numMapTasks = numMapTasks;
	}

	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;		
	}

	public void setProperties(Hashtable<String, String> properties) {
		this.properties = properties;
	}

	public void setReducerClass(Class<?> reduceClass) {
		this.reduceClass = reduceClass.getName();
		this.hasReduceClass = true;
	}

	public void setReducerSelectorClass(Class<?> reducerSelectorClass) {
		this.reducerSelectorClass = reducerSelectorClass.getName();
	}
}
