/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import java.util.Properties;
import java.util.Set;

import im.webuzz.threadpool.ThreadPoolExecutorConfig;

/**
 * Configuration for Piled server.
 * 
 * @author zhourenjian
 */
public class PiledConfig {

	/**
	 * Server listening address. Server can be set to listen on given IP or all IPs.
	 * By default no specific address is given, server will listen on all IPs.
	 */
	public static String address = null;
	
	/**
	 * Server port. Default port is 80.
	 * Server might need to be run by root user to access port 80 or other
	 * port which is less 1024.  
	 */
	public static int port = 80;
	
	/**
	 * HTTP worker class name.
	 */
	public static String worker;
	
	/**
	 * Server and its workers monitor class name.
	 */
	public static String serverWorkersMonitor;
	
	/**
	 * Another server class name. By default, this is PiledSSLServer
	 */
	public static String extraServer = "im.webuzz.piled.PiledSSLServer";

	/**
	 * Define configuration manager class name. By default, this is Simple Config.
	 * The class should has at least 3 methods:
	 * #initialize(); #initialize(String); #registerClassListener(Class);
	 */
	public static String configClassName = "im.webuzz.config.Config";
	
//	/**
//	 * All existed configuration class names.
//	 * @see im.webuzz.config.Config
//	 */
//	public static String[] configs;
	
	/**
	 * All Pilet class names.
	 * Pilet is similar to servlet.
	 * @see im.webuzz.pilet.IPilet
	 */
	public static String[] pilets;
	
	/**
	 * All wrapper class names.
	 * Wrapper is used to do jobs on before server starting or after
	 * server stopping.
	 * @see im.webuzz.pilet.IPiledWrapping
	 */
	public static String[] wrappers;
	
	/**
	 * All filter class names.
	 * Filter is used to give a quick response for HTTP requests without
	 * a thread. No CPU-heavy or IO-heavy jobs for filter.
	 * @see im.webuzz.pilet.IFilter
	 */
	public static String[] filters;

	/**
	 * Socket backlog. The backlog argument is the requested maximum number
	 * of pending connections on the socket.
	 */
	public static int socketBacklog = 0;
	
	/**
	 * Each worker work on a CPU die. It should be less than the total dies
	 * of the server's CPU.
	 *  1+ : Set exactly specified workers
	 *  0  : Use system CPU cores
	 * -1  : System CPU cores number - this number's absolute value
	 */
	public static int httpWorkers = 0;
	
	public static ThreadPoolExecutorConfig httpWorkerPool = new ThreadPoolExecutorConfig();
	
	static {
		/**
		 * Core thread number. Core threads will be kept in thread pool to
		 * make server more responsible.
		 */
		httpWorkerPool.coreThreads = 20;
		
		/**
		 * Max thread number. Server will allow this number of threads at the
		 * peak. Default to 128. If set to -1, if there is no limit.
		 */
		httpWorkerPool.maxThreads = 128;
		
		/**
		 * Keep at least given idle threads to response fast. 
		 */
		httpWorkerPool.idleThreads = 10;
		
		/**
		 * If a thread is idle for given seconds, and thread number is greater
		 * than maxThreads or sslMaxThreads, this thread will be recycled.
		 */
		httpWorkerPool.threadIdleSeconds = 120L;
		
		/**
		 * Allow requests waiting in worker queue, if maxThreads is reached.
		 */
		httpWorkerPool.queueTasks = 100;
		
		/**
		 * Allow core threads to time out or not.
		 */
		httpWorkerPool.threadTimeout = false;
	};
	
	/**
	 * Pilet to serve static resources. By default, it is null, meaning using
	 * StaticResourcePilet. If a new class name is set here, the new class (should
	 * be a Pilet instance) is used to serve static resources. 
	 */
	public static String staticResoucePilet = null;
	
	/**
	 * If server performs as a proxy server, this pilet will be used to do
	 * the proxy work. For normal server, leave this variable to null. 
	 */
	public static String proxyPilet = null;
	
	/**
	 * Support monitoring remote connection or not. If supported, IRequestMonitor
	 * instance will be called back once server detects that connection
	 * is closed by remote side.
	 * 
	 * This feature is currently designed for proxy pilet.
	 */
	public static boolean remoteMonitorSupported = false;
	
//	/**
//	 * If some clients try DDoS, block them. 
//	 */
//	public static String[] blockedIPs = null;
	
	public static int maxPipelineRequests = 8;
	
	/**
	 * Support 256 bytes heart animation for URL "/heart", "/love" and "/<3".
	 * Piled server always shows its beating heart to you and loves you forever.
	 */
	public static boolean support256BytesHeart = true;
	
	/**
	 * Allow sending a request to stop server, if stoppingServerSecret is not null and 8+ length long.
	 */
	public static String stoppingServerSecret = null;
	
	/**
	 * This trusted host can make high risk operations. 
	 */
	public static Set<String> serverTrustedHosts = null;

	public static void update(Properties prop) {
		ThreadPoolExecutorConfig wc = httpWorkerPool;
		if (wc != null) {
			if (wc.coreThreads < 0) {
				wc.coreThreads = 0;
			}
			if (wc.maxThreads > 0 && wc.maxThreads < wc.coreThreads) {
				wc.maxThreads = wc.coreThreads;
			}
			if (wc.idleThreads > 0 && wc.idleThreads > wc.maxThreads) {
				wc.idleThreads = wc.maxThreads;
			}
		}
	}

}
