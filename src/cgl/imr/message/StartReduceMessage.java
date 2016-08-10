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

package cgl.imr.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cgl.imr.base.SerializationException;

public class StartReduceMessage extends PubSubMessage {

	Map<Integer, Integer> reduceInputMap;
	String jobId;
	String reduceTopicBase;

	private StartReduceMessage() {
		reduceInputMap = new HashMap<Integer, Integer>();
	}
	
	public StartReduceMessage(ConcurrentHashMap<Integer, Integer> map,String jobId,String reduceTopicBase){
		this();
		this.jobId=jobId;
		this.reduceTopicBase=reduceTopicBase;
		this.reduceInputMap.putAll(map);
	}

	public StartReduceMessage(byte[] bytes) throws SerializationException {
		this();
		this.fromBytes(bytes);
	}

	public int getNumReduceInputsExpected(int reducerNo) {
		return reduceInputMap.get(reducerNo);
	}
	
	
	public String getReduceTopicBase() {
		return reduceTopicBase;
	}

	public String getJobId(){
		return jobId;
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			// First byte is the message type
			byte msgType = din.readByte();
			if (msgType != START_REDUCE) {
				throw new SerializationException(
						"Invalid set of bytes to deserialize "
								+ this.getClass().getName() + ".");
			}

			// Read the refId if any and set the boolean flag.
			readRefIdIfAny(din);
			
			int len=din.readInt();
			byte[] data=new byte[len];
			din.readFully(data);
			this.jobId=new String(data);
			
			len=din.readInt();
			data=new byte[len];
			din.readFully(data);
			this.reduceTopicBase=new String(data);

			int key;
			int value;
			int count = din.readInt();
			reduceInputMap = new HashMap<Integer, Integer>();
			for (int i = 0; i < count; i++) {
				key = din.readInt();
				value = din.readInt();
				reduceInputMap.put(key, value);
			}

			din.close();
			baInputStream.close();

		} catch (Exception e) {
			throw new SerializationException("Could not load classes", e);
		}

	}

	@Override
	public byte[] getBytes() throws SerializationException {
		byte[] serializedBytes = null;
		try {

			ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(baOutputStream);

			// First byte is the message type.
			dout.writeByte(START_REDUCE);

			// Write the refID if any with the boolean flag.
			serializeRefId(dout);
			byte[] data=jobId.getBytes();
			dout.writeInt(data.length);
			dout.write(data);
			
			data=reduceTopicBase.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			dout.writeInt(reduceInputMap.size());
			Iterator<Integer> ite = reduceInputMap.keySet().iterator();
			int key;
			while (ite.hasNext()) {
				key = ite.next();
				dout.writeInt(key);
				dout.writeInt(reduceInputMap.get(key));
			}

			dout.flush();
			dout.close();
			serializedBytes = baOutputStream.toByteArray();
			baOutputStream = null;
			dout = null;

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return serializedBytes;
	}

}
