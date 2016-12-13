/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Data structure for request of changing NIO socket.
 * 
 * @author zhourenjian
 *
 */
class ChangeRequest {
	public static final int REGISTER = 1;
	public static final int CHANGEOPS = 2;
	public static final int CLOSE = 4;
	public static final int CLEAN = 8; // Cleaan expired or lost connections
	
	public SocketChannel socket;
	public int type;
	public int ops;
	public ByteBuffer data;

	public ChangeRequest(SocketChannel socket, int type) {
		this.socket = socket;
		this.type = type;
	}

	public ChangeRequest(SocketChannel socket, int type, int ops) {
		this.socket = socket;
		this.type = type;
		this.ops = ops;
	}
	
	public ChangeRequest(SocketChannel socket, int type, int ops, ByteBuffer data) {
		this.socket = socket;
		this.type = type;
		this.ops = ops;
		this.data = data;
	}
	
}
