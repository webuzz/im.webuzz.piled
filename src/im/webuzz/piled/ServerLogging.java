/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import im.webuzz.config.Config;
import im.webuzz.pilet.IPiledServer;
import im.webuzz.pilet.IPiledWrapping;

/**
 * Replace system's stdout and stderr with file logging.
 * 
 * ServerLogging needs to be configured in piled.ini:
 * wrappers=im.webuzz.piled.ServerLogging;...
 * logPath=/t/piled/piled.log
 * 
 * if not listed in wrappers or logPath is not configured, server logging
 * will be printed to stdout or stderr.
 * 
 * @author zhourenjian
 *
 */
public class ServerLogging implements IPiledWrapping {

	private static boolean initialized = false;
	
	private String logPath;
	private OutputStream fos;

	@Override
	public void beforeStartup(IPiledServer server) {
		if (initialized) {
			return;
		}
		Config.registerUpdatingListener(LoggingConfig.class);
		
		logPath = LoggingConfig.logPath;
		if (logPath != null) {
			if (logPath.endsWith(".log")) {
				String suffix = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
				logPath = logPath.substring(0, logPath.length() - 4) + "." + suffix + ".log";
			}
			File logFile = new File(logPath);
			File logFolder = logFile.getParentFile();
			if (!logFolder.exists()) {
				logFolder.mkdirs();
			}
			try {
				fos = new FileOutputStream(logFile, true);
				if (LoggingConfig.logBufferedSize > 0) {
					fos = new BufferedOutputStream(fos, LoggingConfig.logBufferedSize);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if (fos != null) {
				PrintStream logPS = new PrintStream(fos, LoggingConfig.logAutoFlush);
				System.setErr(logPS);
				System.setOut(logPS);
			}
		}
		System.out.println(new Date());
		System.out.println("Server starting.");
		
		Thread thread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// Thread safe SimpleDateFormat
				SimpleDateFormat logFileDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				while (true) {
					long leftMS = new Date().getTime() % 60000;
					if (leftMS < 1000) {
						leftMS = 1000;
					}
					try {
						Thread.sleep(leftMS);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					String path = LoggingConfig.logPath;
					if (path != null) {
						if (path.endsWith(".log")) {
							String suffix = logFileDateFormat.format(new Date());
							path = path.substring(0, path.length() - 4) + "." + suffix + ".log";
						}
						if (!path.equals(logPath)) {
							File logFile = new File(path);
							File logFolder = logFile.getParentFile();
							if (!logFolder.exists()) {
								logFolder.mkdirs();
							}
							OutputStream newFOS = null;
							try {
								newFOS = new FileOutputStream(logFile, true);
								if (LoggingConfig.logBufferedSize > 0) {
									newFOS = new BufferedOutputStream(newFOS, LoggingConfig.logBufferedSize);
								}
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							}
							if (newFOS != null) {
								System.out.println(new Date());
								System.out.println("Server log rotated!");
								PrintStream logPS = new PrintStream(newFOS, LoggingConfig.logAutoFlush);
								System.setErr(logPS);
								System.setOut(logPS);
								
								if (fos != null) {
									try {
										fos.close();
									} catch (IOException e) {
										e.printStackTrace();
									}
								}
								fos = newFOS;
								logPath = path;
							}
						} // end of not equal log path
					} // end of path != null
				} // end of while
			}
			
		}, "Piled Log Rotator");
		thread.setDaemon(true);
		thread.start();
		
		initialized = true;
	}

	@Override
	public void afterClosed(IPiledServer server) {
		System.out.println(new Date());
		System.out.println("Server stopped!");
		if (fos != null) {
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
