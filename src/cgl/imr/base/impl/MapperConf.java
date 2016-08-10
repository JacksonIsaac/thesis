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

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterException;
import cgl.imr.base.TwisterSerializable;
import cgl.imr.base.Value;
import cgl.imr.data.DataPartition;
import cgl.imr.data.DataPartitioner;

/**
 * Configuration for Map tasks. <code>MapTaskConf</code> relays the <key, value>
 * information and the information about the associated data partition.
 * 
 * 
 * @author Jaliya Ekanayake (jaliyae@gamil.com, jekanaya@cs.indiana.edu)
 * 
 */
public class MapperConf implements TwisterSerializable {

	private ClassLoader classLoader;
	private boolean hasDataPartition = false;
	private boolean hasValue = false;

	private int mapTaskNo = 0;
	private DataPartition partition = null;

	private String partitionClass = null;
	private String valClass;

	private Value value;

	private MapperConf() {
	}

	public MapperConf(byte[] bytes, ClassLoader classLoader)
			throws SerializationException {
		this();
		this.classLoader = classLoader;
		this.fromBytes(bytes);

	}

	public MapperConf(int taskNo) {
		this();
		this.mapTaskNo = taskNo;
	}

	public MapperConf(int taskNo, DataPartition dataPartition) {
		this(taskNo);
		this.hasDataPartition = true;
		this.partition = dataPartition;
		this.partitionClass = this.partition.getClass().getName();
	}

	public MapperConf(int taskNo, DataPartition dataPartition, Value value) {
		this(taskNo, value);
		this.hasDataPartition = true;
		this.partition =dataPartition;// dataPartitioner.getPartition(taskNo);
		this.partitionClass = this.partition.getClass().getName();
	}

	public MapperConf(int taskNo, Value value) {
		this(taskNo);
		this.value = value;
		this.hasValue = true;
		this.valClass = value.getClass().getName();
	}

	public void fromBytes(byte[] bytes) throws SerializationException {
		ByteArrayInputStream baInputStream = new ByteArrayInputStream(bytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			int len = 0;
			byte[] data = null;
			this.mapTaskNo = din.readInt();
			this.hasValue = din.readBoolean();

			if (this.hasValue) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.valClass = new String(data);

				Class<?> c = null;
				try {
					c = Class.forName(valClass, true, classLoader);
					this.value = (Value) c.newInstance();
				} catch (Exception e) {
					throw new SerializationException(
							"Could not load the class.", e);
				}
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.value.fromBytes(data);
			}

			this.hasDataPartition = din.readBoolean();
			if (this.hasDataPartition) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.partitionClass = new String(data);

				Class<?> c;
				try {
					c = Class.forName(partitionClass, true, classLoader);
					this.partition = (DataPartition) c.newInstance();
				} catch (Exception e) {
					throw new SerializationException(
							"Exception @ MapTaskRequest", e);
				}

				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.partition.fromBytes(data);
			}

			din.close();
			baInputStream.close();
		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	public byte[] getBytes() throws SerializationException {
		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;
		try {

			byte[] data = null;

			dout.writeInt(this.mapTaskNo);
			dout.writeBoolean(this.hasValue);

			if (this.hasValue) {
				data = valClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				data = value.getBytes();
				dout.writeInt(data.length);
				dout.write(data);
			}

			dout.writeBoolean(this.hasDataPartition);

			if (this.hasDataPartition) {
				data = this.partitionClass.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

				data = this.partition.getBytes();
				dout.writeInt(data.length);
				dout.write(data);

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

	public DataPartition getDataPartition() {
		return partition;
	}

	public int getMapTaskNo() {
		return mapTaskNo;
	}

	public Value getValue() {
		return value;
	}

	public boolean isHasDataPartition() {
		return hasDataPartition;
	}

	public boolean isHasValue() {
		return hasValue;
	}

	public void setValue(Value value) throws TwisterException {
		if (value == null) {
			throw new TwisterException("Value cannot be null.");
		}
		this.value = value;
		this.hasValue = true;
		this.valClass = value.getClass().getName();
	}
}
