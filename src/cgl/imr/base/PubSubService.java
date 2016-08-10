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

package cgl.imr.base;

/**
 * Captures the behaviors expected from a pub-sub service. Any implementation of
 * this interface would be usable by i-MapRedcue framework.
 *
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu)
 *
 */
public interface PubSubService {
	/**
	 * Close the PubSubService connections.
	 *
	 * @throws PubSubException
	 */
	public void close() throws PubSubException;

	/**
	 * Publish a <code>byte[]</code> message to a given topic.
	 *
	 * @param topic
	 *            - Topic to send the message.
	 * @param message
	 *            - Message to be sent as a <code>byte[]</code> array.
	 * @throws PubSubException
	 */
	public void send(String topic, byte[] message) throws PubSubException;

	/**
	 * Publish a <code>String</code> message to a given topic.
	 *
	 * @param topic
	 *            - Topic to send the message.
	 * @param message
	 *            - Message to be sent.
	 * @throws PubSubException
	 */
	public void send(String topic, String message) throws PubSubException;

	/**
	 * Sets the subscribable object for this PubSubService. This supports one
	 * subscribable object per connection. The specific messages are filtered
	 * using the information in the message.
	 *
	 * @param callback
	 *            - Subscribable object.
	 * @throws PubSubException
	 */
	public void setSubscriber(Subscribable callback) throws PubSubException;

	/**
	 * Subscribe to a topic.
	 *
	 * @param topic
	 *            - Topic to be subscribed to.
	 * @throws PubSubException
	 */
	public void subscribe(String topic) throws PubSubException;

	/**
	 * Unsubscribe from a topic for which we have subscribed previously.
	 *
	 * @param topic
	 *            - Topic to to be unsubscribed.
	 * @throws PubSubException
	 */
	public void unsubscribe(String topic) throws PubSubException;

}
