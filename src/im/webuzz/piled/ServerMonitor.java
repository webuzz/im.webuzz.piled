/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import im.webuzz.threadpool.ThreadPoolExecutorConfig;

/**
 * HTTP KeepAlive monitor, HTTP connection monitor and hot deployment monitor.
 * 
 * Send out a heart beat to Piled server every 10 seconds, and Piled
 * server will test all existed keep alive connections whether it is
 * expired or not.
 * 
 * Send signals to server to clean expired or hanged connections. And re-initialize
 * configuration if there are updates.
 * 
 * @author zhourenjian
 *
 */
class ServerMonitor implements Runnable {

	private PiledAbstractServer server;
	
	public ServerMonitor(PiledAbstractServer server) {
		super();
		this.server = server;
	}

	@Override
	public void run() {
		int count = 0;
		ThreadPoolExecutorConfig lastConfig = PiledConfig.httpWorkerPool;
		while (server.isRunning()) {
			for (int i = 0; i < 5; i++) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (!server.isRunning()) {
					break;
				}
			}

			ThreadPoolExecutorConfig wc = PiledConfig.httpWorkerPool;
			// Keep-Alive monitor
			for (HttpWorker worker : server.workers) {
				synchronized (worker.queue) {
					/**
					 * @see HttpWorker#run
					 */
					worker.queue.add(new ServerDataEvent(null, null, 0));
					worker.queue.notify();
				}
				if (wc != null) {
					wc.updatePoolWithComparison(worker.workerPool, lastConfig);
				}
			}
			if (wc != null) {
				lastConfig = wc;
			}

			count++;
			if (count % 6 == 5) { // Every 60s
				// Connection monitor
				server.send(null, null, 0, -1); // clean expired or hanged connections
				
				// Hot deployment
				server.initializeWrapperInstances();
				server.initializeFilterInstances();
				server.initializePiletInstances();
			}
		} // end of while
	}
	
}
