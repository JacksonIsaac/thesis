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

package cgl.imr.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cgl.imr.base.SerializationException;

public class TaskStatus extends PubSubMessage {

	private String exceptionString;
	private long execuationTime;
	private boolean hasException;
	private byte status;
	private int taskNo;
	private byte taskType;
	private int iteration;
	private boolean hasReduceInputMap=false;
	private Map<Integer,Integer> reduceInputMap;

	private TaskStatus() {
	}

	public TaskStatus(byte taskType, byte status, int taskNo,
			long execuationTime, int iteration) {
		super();
		this.taskType = taskType;
		this.status = status;
		this.taskNo = taskNo;
		this.execuationTime = execuationTime;
		this.iteration = iteration;

	}
	

	public Map<Integer, Integer> getReduceInputMap() {
		return reduceInputMap;
	}

	public void setReduceInputMap(Map<Integer, Integer> reduceInputMap) {
		this.hasReduceInputMap=true;
		this.reduceInputMap = reduceInputMap;		
	}

	public boolean isHasReduceInputMap() {
		return hasReduceInputMap;
	}

	public TaskStatus(byte[] data) throws SerializationException {
		this();
		this.fromBytes(data);
	}

	@Override
	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);
		int len = 0;
		byte[] data = null;
		try {
			// First byte is the message type
			byte msgType = din.readByte();
			if (msgType != TASK_STATUS) {
				throw new SerializationException(
						"Invalid set of bytes to deserialize "
								+ this.getClass().getName() + ".");
			}

			// read the task type
			taskType = din.readByte();
			status = din.readByte();
			taskNo = din.readInt();
			iteration = din.readInt();
			execuationTime = din.readLong();
			hasException = din.readBoolean();
			if (hasException) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				exceptionString = new String(data);
			}
			
			hasReduceInputMap=din.readBoolean();			
			if (hasReduceInputMap) {
				int key;
				int value;
				int count=din.readInt();
				reduceInputMap=new HashMap<Integer,Integer>();
				for(int i=0;i<count;i++){
					key=din.readInt();
					value=din.readInt();
					reduceInputMap.put(key,value);
				}
			}

			din.close();
			baInputStream.close();

		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] serializedBytes = null;
		byte[] data = null;
		try {
			// First byte is the message type.
			dout.writeByte(TASK_STATUS);
			dout.writeByte(taskType);
			dout.writeByte(status);
			dout.writeInt(taskNo);
			dout.writeInt(iteration);
			dout.writeLong(execuationTime);
			dout.writeBoolean(hasException);
			if (hasException) {
				data = exceptionString.getBytes();
				dout.writeInt(data.length);
				dout.write(data);
			}
			
			dout.writeBoolean(hasReduceInputMap);			
			if (hasReduceInputMap) {			
				dout.writeInt(reduceInputMap.size());
				Iterator<Integer> ite=reduceInputMap.keySet().iterator();
				int key;
				while(ite.hasNext()){
					key=ite.next();
					dout.writeInt(key);
					dout.writeInt(reduceInputMap.get(key));
				}				
			}

			dout.flush();
			serializedBytes = baOutputStream.toByteArray();
			dout.close();			
			baOutputStream = null;
			dout = null;

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
		return serializedBytes;
	}

	public String getExceptionString() {
		return exceptionString;
	}

	public long getExecuationTime() {
		return execuationTime;
	}

	public int getStatus() {
		return status;
	}

	public int getTaskNo() {
		return taskNo;
	}

	public int getIteration() {
		return iteration;
	}

	public int getTaskType() {
		return taskType;
	}

	public boolean isHasException() {
		return hasException;
	}

	public void setExceptionString(String exceptionString) {
		this.hasException = true;
		this.exceptionString = exceptionString;
	}

	public void setExecuationTime(long execuationTime) {
		this.execuationTime = execuationTime;
	}

	public void setStatus(byte status) {
		this.status = status;
	}

	public void setTaskNo(int taskNo) {
		this.taskNo = taskNo;
	}

	public void setTaskType(byte taskType) {
		this.taskType = taskType;
	}
}
