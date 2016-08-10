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

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterConstants;

/**
 * Response coming from the daemons to the <code>TwisterDriver.</code>
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 * 
 */
public class WorkerResponse extends PubSubMessage {

	private String exceptionString;
	private int daemonNo;
	private String daemonIP;

	public WorkerResponse(byte[] bytes) throws SerializationException {
		super();
		fromBytes(bytes);
	}

	public WorkerResponse() {
	}

	public WorkerResponse(int daemonNo, String daemonIP) {
		super();
		this.daemonNo = daemonNo;
		this.daemonIP = daemonIP;
	}

	public int getDaemonNo() {
		return daemonNo;
	}

	public String getDaemonIp() {
		return daemonIP;
	}

	@Override
	public void fromBytes(byte[] messageBytes) throws SerializationException {

		ByteArrayInputStream baInputStream = new ByteArrayInputStream(
				messageBytes);
		DataInputStream din = new DataInputStream(baInputStream);

		try {
			byte[] data = null;
			int len = 0;

			// First byte is the message type
			byte msgType = din.readByte();
			if (msgType != WORKER_RESPONSE) {
				throw new SerializationException(
						"Invalid set of bytes to deserialize "
								+ this.getClass().getName() + ".");
			}

			// Read the refId if any and set the boolean flag.
			readRefIdIfAny(din);
			daemonNo = din.readInt();
			len = din.readInt();
			data = new byte[len];
			din.readFully(data);
			this.daemonIP = new String(data);

			boolean hasException = din.readBoolean();
			if (hasException) {
				len = din.readInt();
				data = new byte[len];
				din.readFully(data);
				this.exceptionString = new String(data);
			}

			din.close();
			baInputStream.close();

		} catch (IOException ioe) {
			throw new SerializationException(ioe);
		}
	}

	@Override
	public byte[] getBytes() throws SerializationException {

		ByteArrayOutputStream baOutputStream = new ByteArrayOutputStream();

		DataOutputStream dout = new DataOutputStream(baOutputStream);
		byte[] marshalledBytes = null;

		try {
			byte[] data = null;
			dout.writeByte(TwisterConstants.WORKER_RESPONSE);

			// Write the refID if any with the boolean flag.
			serializeRefId(dout);
			dout.writeInt(daemonNo);
			data = daemonIP.getBytes();
			dout.writeInt(data.length);
			dout.write(data);

			dout.writeBoolean(isHasException());
			if (isHasException()) {
				data = exceptionString.getBytes();
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

	public String getExceptionString() {
		return exceptionString;
	}

	public boolean isHasException() {
		return (exceptionString != null) ? true : false;
	}

	public void setExceptionString(String exceptionString) {
		this.exceptionString = exceptionString;
	}
}
