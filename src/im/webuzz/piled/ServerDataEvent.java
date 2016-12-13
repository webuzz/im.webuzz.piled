/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import im.webuzz.pilet.IPiledWorker;

import java.nio.channels.SocketChannel;

/**
 * Server data event for notifying server to send data or do other jobs.
 * 
 * @author zhourenjian
 *
 */
class ServerDataEvent {
	public IPiledWorker worker;
	public SocketChannel socket;
	public byte[] data;
	public int count;
	
	public ServerDataEvent(SocketChannel socket, byte[] data, int count) {
		this.socket = socket;
		this.data = data;
		this.count = count;
	}
}