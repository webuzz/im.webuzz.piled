package im.webuzz.piled;

import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Map.Entry;

public class SocketCloser implements Runnable {

	private PiledAbstractServer server;
	
	public SocketCloser(PiledAbstractServer server) {
		super();
		this.server = server;
	}

	@Override
	public void run() {
		//int count = 0;
		while (server.isRunning()) {
			synchronized (server.closingMutex) {
				try {
					server.closingMutex.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (Entry<SocketChannel, Socket> entry : server.closingSockets.entrySet()) {
				SocketChannel channel = entry.getKey();
				Socket socket = entry.getValue();
				if (socket != null) {
					try {
						socket.shutdownOutput();
					} catch (Exception e1) {
						//e1.printStackTrace();
					}
					try {
						socket.shutdownInput();
					} catch (Exception e1) {
						//e1.printStackTrace();
					}
					try {
						socket.close();
					} catch (Exception e1) {
						//e1.printStackTrace();
					}
				}
				server.closingSockets.remove(channel);
			}
		}		
	}

}
