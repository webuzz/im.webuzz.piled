/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

//import im.webuzz.config.Config;
import im.webuzz.pilet.HttpConfig;
import im.webuzz.pilet.HttpLoggingConfig;
import im.webuzz.pilet.MIMEConfig;
import im.webuzz.threadpool.SimpleNamedThreadFactory;
import im.webuzz.threadpool.SimpleThreadPoolExecutor;
import im.webuzz.threadpool.ThreadPoolExecutorConfig;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

public class PiledServer extends PiledAbstractServer {

	// The buffer into which we'll read data when it's available
	private ByteBuffer readBuffer = ByteBuffer.allocate(8192);

	public PiledServer(InetAddress hostAddress, int port, HttpWorker[] workers) {
		super(hostAddress, port, false, workers);
	}
	
	@Override
	protected void bindWorkers(HttpWorker[] workers) {
		this.workers = workers;
		if (this.workers != null) {
			ThreadPoolExecutorConfig wc = PiledConfig.httpWorkerPool;
			if (wc == null) {
				wc = new ThreadPoolExecutorConfig();
			}
			int count = workers.length;
			for (int i = 0; i < count; i++) {
				SimpleThreadPoolExecutor executor =  new SimpleThreadPoolExecutor(wc,
				                new SimpleNamedThreadFactory("HTTP Service Worker" + (count == 1 ? "" : "-" + (i + 1))));
				executor.allowCoreThreadTimeOut(wc.threadTimeout);
				this.workers[i].bindingServer(this, executor);
			}
		}
	}
	
	protected void accept(SelectionKey key) throws IOException {
		// For an accept to be pending the channel must be a server socket channel.
		ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
		if (serverSocketChannel == null) {
			return;
		}
		// Accept the connection and make it non-blocking
		SocketChannel socketChannel = serverSocketChannel.accept();
		if (socketChannel == null) {
			return;
		}
		Socket socket = socketChannel.socket();
		if (socket == null) {
			return;
		}
		//socket.setSoLinger(false, 0);
		socket.setTcpNoDelay(true);
		socketChannel.configureBlocking(false);

		// Register the new SocketChannel with our Selector, indicating
		// we'd like to be notified when there's data waiting to be read
		socketChannel.register(this.selector, SelectionKey.OP_READ, new Long(System.currentTimeMillis()));
		//System.out.println("[*]+ Registering socket 1 " + socketChannel);
	}

	protected void read(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		// Clear out our read buffer so it's ready for new data
		this.readBuffer.clear();

		// Attempt to read off the channel
		int numRead;
		try {
			numRead = socketChannel.read(this.readBuffer);
		} catch (IOException e) {
			String message = e.getMessage();
			if (message.indexOf("Connection reset by peer") == -1
					&& message.indexOf("Connection timed out") == -1
					&& message.indexOf("Broken pipe") == -1
					&& message.indexOf("closed by the remote host") == -1
					&& message.indexOf("connection was aborted") == -1
					&& message.indexOf("No route to host") == -1) {
				e.printStackTrace();
			}
			// The remote forcibly closed the connection, cancel
			// the selection key and close the channel.
			numRead = -1;
		}

		if (numRead == -1) {
			// Remote entity shut the socket down cleanly. Do the
			// same from our end and cancel the channel.
			closeChannel(key, socketChannel, false);
		} else {
			// Hand the data off to our worker thread
			workers[socketChannel.hashCode() % workers.length].processData(socketChannel,
					this.readBuffer.array(), this.readBuffer.arrayOffset(), numRead);
		}
	}

	protected void write(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();

		List<ByteBuffer> queue = (List<ByteBuffer>) this.pendingData.get(socketChannel);
		if (queue == null) {
			key.interestOps(SelectionKey.OP_READ);
			return;
		}
		
		boolean filled = false;
		int totalWritten = 0;
		// Write until there's no more data ...
		while (!queue.isEmpty()) {
			ByteBuffer buf = (ByteBuffer) queue.get(0);
			if (buf.capacity() == 0) {
				pendingData.remove(socketChannel);
				queue.remove(0);
				closeChannel(key, socketChannel, true);
				// Not notify any data written back to request
				return;
			}
			try {
				int numberWritten = socketChannel.write(buf);
				if (numberWritten > 0) {
					totalWritten += numberWritten;
				}
			} catch (Throwable e) {
				String message = e.getMessage();
				if (message.indexOf("Connection reset by peer") == -1
						&& message.indexOf("Connection timed out") == -1
						&& message.indexOf("Broken pipe") == -1
						&& message.indexOf("closed by the remote host") == -1
						&& message.indexOf("connection was aborted") == -1
						&& message.indexOf("No route to host") == -1
						&& message.indexOf("connection was forcibly closed") == -1) {
					e.printStackTrace();
				}
				queue.remove(0);
				closeChannel(key, socketChannel, false);
				// Not notify any data written back to request
				return;
			}
			if (buf.remaining() > 0) {
				// ... or the socket's buffer fills up
				filled = true;
				break;
			}
			queue.remove(0);
		}
		if (totalWritten > 0) {
			workers[socketChannel.hashCode() % this.workers.length].processData(socketChannel,
					null, 0, filled ? totalWritten : -totalWritten); // notify data written, negative count means all queued data written
		}
		
		if (queue.isEmpty()) {
			// We wrote away all data, so we're no longer interested
			// in writing on this socket. Switch back to waiting for
			// data.
			key.interestOps(SelectionKey.OP_READ);
		}
	}

	public static void main(String[] args) {
		try {
			Class<?> clazz = Class.forName(PiledConfig.configClassName);
			if (clazz != null) {
				Method initMethod = args != null && args.length > 0 ? clazz.getMethod("initialize", String.class) : clazz.getMethod("initialize");
				if (initMethod != null && (initMethod.getModifiers() & Modifier.STATIC) != 0) {
					if (args != null && args.length > 0) {
						initMethod.invoke(clazz, args[0]);
					} else {
						initMethod.invoke(clazz);
					}
				}
				Method registerMethod = clazz.getMethod("registerUpdatingListener", Class.class);
				if (registerMethod != null && (registerMethod.getModifiers() & Modifier.STATIC) != 0) {
					registerMethod.invoke(clazz, HttpConfig.class);
					registerMethod.invoke(clazz, HttpLoggingConfig.class);
					registerMethod.invoke(clazz, MIMEConfig.class);
					registerMethod.invoke(clazz, PiledConfig.class);
				}
			}
		} catch (ClassNotFoundException e) {
			System.out.println("[WARN]: Class " + PiledConfig.configClassName + " is not found. Server may not be configurable.");
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println("[WARN]: There are errors. Server may not be configurable.");
		}
		/*
		if (args != null && args.length > 0) {
			Config.initialize(args[0]);
		} else {
			Config.initialize();
		}
		Config.registerUpdatingListener(HttpConfig.class);
		Config.registerUpdatingListener(HttpLoggingConfig.class);
		Config.registerUpdatingListener(MIMEConfig.class);
		Config.registerUpdatingListener(PiledConfig.class);
		// */
				
		int workerCount = PiledConfig.httpWorkers;
		if (workerCount <= 0) {
			workerCount = Runtime.getRuntime().availableProcessors() + workerCount;
			if (workerCount <= 0) {
				workerCount = 1;
			}
		}
		
		HttpWorker[] workers = new HttpWorker[workerCount];
		boolean created = false;
		String workerClass = PiledConfig.worker;
		if (workerClass != null && workerClass.length() > 0) {
			try {
				Class<?> clazz = Class.forName(workerClass);
				try {
					Method initMethod = clazz.getMethod("initialize");
					if (initMethod != null && (initMethod.getModifiers() & Modifier.STATIC) != 0) {
						initMethod.invoke(clazz);
					}
				} catch (SecurityException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				if (clazz != null && HttpWorker.class.isAssignableFrom(clazz)) {
					for (int i = 0; i < workers.length; i++) {
						workers[i] = (HttpWorker) clazz.newInstance();
					}
					created = true;
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		if (!created) {
			for (int i = 0; i < workers.length; i++) {
				workers[i] = new HttpWorker();
			}
		}
		String address = PiledConfig.address;
		InetAddress serverAddress = null;
		if (address != null && address.length() > 0 && !"*".equals(address)) {
			try {
				serverAddress = InetAddress.getByName(address);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		PiledServer server = new PiledServer(serverAddress, PiledConfig.port, workers);

		try {
			server.init();

			server.initializeWrapperInstances();
			server.initializeFilterInstances();
			server.initializePiletInstances();

			for (int i = 0; i < workers.length; i++) {
				Thread workerThread = new Thread(workers[i], "Piled HTTP Worker" + (workers.length > 1 ? "-" + (i + 1) : ""));
				workerThread.setDaemon(true);
				workerThread.setPriority(Thread.MAX_PRIORITY - 1);
				workerThread.start();
			}
			
			String workersMonitor = PiledConfig.serverWorkersMonitor;
			if (workersMonitor != null && workersMonitor.length() > 0) {
				try {
					Class<?> clazz = Class.forName(workersMonitor);
					if (clazz != null) {
						Constructor<?> constructor = clazz.getConstructor(PiledAbstractServer.class, HttpWorker[].class);
						if (constructor != null) {
							Object r = constructor.newInstance(server, workers);
							if (r instanceof Runnable) {
								Thread pipeThread = new Thread((Runnable) r, "Piled HTTP Piper");
								pipeThread.setDaemon(true);
								pipeThread.start();
							}
						}
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (InstantiationException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
			
			ServerMonitor serverMonitor = new ServerMonitor(server);
			Thread monitorThread = new Thread(serverMonitor, "Piled HTTP Monitor");
			monitorThread.setDaemon(true);
			monitorThread.start();
			
			SocketCloser socketCloser = new SocketCloser(server);
			Thread socketThread = new Thread(socketCloser, "Piled HTTP Closer");
			socketThread.setDaemon(true);
			socketThread.start();
			
			String extraServerClass = PiledConfig.extraServer;
			if (extraServerClass != null && extraServerClass.length() > 0) {
				try {
					Class<?> clazz = Class.forName(extraServerClass);
					Method runMethod = clazz.getMethod("extraRun", int.class);
					if (runMethod != null && (runMethod.getModifiers() & Modifier.STATIC) != 0) {
						runMethod.invoke(clazz, workerCount);
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (SecurityException e) {
					e.printStackTrace();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
			}
			
			try {
				Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
				Thread.currentThread().setName("Piled HTTP Server");
			} catch (Throwable e) {
				System.out.println("Update main thread priority failed!");
				e.printStackTrace();
			}
			
			server.runLoop();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		
		server.closeWrapperInstances();
	}

}
