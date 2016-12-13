/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

/**
 * Logging configuration for server stdout and stderr output.
 * 
 * @author zhourenjian
 * @see ServerLogging
 */
public class LoggingConfig {
	
	public static String logPath;

	/**
	 * Caching logging until it reaches this buffer maximum size.
	 */
	public static int logBufferedSize = 1024;
	
	/**
	 * Flush method is automatically invoked after a byte array is written, one
	 * of the println methods is invoked, or a newline character or byte ('\n')
	 * is written.
	 */
	public static boolean logAutoFlush = false;
	
}
