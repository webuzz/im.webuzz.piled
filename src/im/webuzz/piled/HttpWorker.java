/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import im.webuzz.pilet.HttpConfig;
import im.webuzz.pilet.HttpLoggingUtils;
import im.webuzz.pilet.HttpQuickResponse;
import im.webuzz.pilet.HttpRequest;
import im.webuzz.pilet.HttpResponse;
import im.webuzz.pilet.HttpWorkerUtils;
import im.webuzz.pilet.ICloneablePilet;
import im.webuzz.pilet.IFilter;
import im.webuzz.pilet.IPiledServer;
import im.webuzz.pilet.IPiledWorker;
import im.webuzz.pilet.IPilet;
import im.webuzz.pilet.IRequestMonitor;

import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * HTTP worker to deal all HTTP connections.
 * 
 * @author zhourenjian
 */
public class HttpWorker implements Runnable, IPiledWorker {
	
	static byte[] ZERO_BYTES = new byte[0];
	
	protected Map<SocketChannel, HttpRequest> closeSockets = new ConcurrentHashMap<SocketChannel, HttpRequest>();

	List<ServerDataEvent> queue = new LinkedList<ServerDataEvent>();
	Map<SocketChannel, HttpRequest> requests = new HashMap<SocketChannel, HttpRequest>();
	protected ThreadPoolExecutor workerPool;
	long totalRequests;
	long errorRequests;
	
	protected PiledAbstractServer server;
	
	public void removeSocket(SocketChannel socket) {
		processData(socket, null, 0, 0);
	}
	
	public void processData(SocketChannel socket, byte[] data, int offset, int count) {
		byte[] dataCopy = null;
		if (data != null) {
			dataCopy = new byte[count];
			if (count > 0) {
				System.arraycopy(data, offset, dataCopy, 0, count);
			}
		} else {
			dataCopy = ZERO_BYTES;
		}
		//new RuntimeException().printStackTrace();
		synchronized(queue) {
			queue.add(new ServerDataEvent(socket, dataCopy, count));
			queue.notify();
		}
	}

	public void bindingServer(PiledAbstractServer server, ThreadPoolExecutor executor) {
		this.server = server;
		workerPool = executor;
	}
	
	@Override
	public IPiledServer getServer() {
		return server;
	}
	
	/*
	 * Check expired requests and close it gracefully. Invoked every 10s
	 */
	protected void checkExpiredRequests() {
		boolean monitorSupported = PiledConfig.remoteMonitorSupported;
		long now = System.currentTimeMillis();
		Set<HttpRequest> toRemoveds = new HashSet<HttpRequest>();
		for (HttpRequest req : requests.values()) {
			if (req.done) {
				if (/*(req.keepAliveMax <= 1 || req.requestCount >= req.keepAliveMax) // HTTP connection closed
						&& */now - Math.max(req.created, req.lastSent) > req.keepAliveTimeout * 1000) { // timeout
					toRemoveds.add(req);
				} else if ((req.keepAliveMax <= 1 || req.requestCount >= req.keepAliveMax) // HTTP connection closed
						&& req.created < req.lastSent && now - req.lastSent > 5000) { // data sent
					toRemoveds.add(req);
				}
			} else { // not finished response yet
				if (!req.comet && now - Math.max(req.created, req.lastSent) > 2 * HttpConfig.aliveTimeout * 1000) { // normally timeout
					toRemoveds.add(req);
				}
			}
		}
		for (HttpRequest req : closeSockets.values()) {
			if (req.created < req.lastSent && now - req.created > req.keepAliveTimeout * 1000) {
				toRemoveds.add(req);
			} else if (now - req.created > 2 * req.keepAliveTimeout * 1000) {
				toRemoveds.add(req);
			}
		}

		final List<IRequestMonitor> monitors = monitorSupported ? new ArrayList<IRequestMonitor>() : null;
		
		for (HttpRequest req : toRemoveds) {
			/*HttpRequest r = */requests.remove(req.socket);
			/*
			if (r != null && !r.done) {
				HttpWorkerUtils.send408RequestTimeout(req, dataEvent, closeSockets);
				HttpLoggingUtils.addLogging(req.host, req, 408, 0);
			}
			*/
			/*r = */closeSockets.remove(req.socket);
			/*
			if (r == null) {
				closeSockets.remove(null);
			}
			*/
			if (monitorSupported && !req.done && req.monitor != null) {
				monitors.add(req.monitor);
			}
			// notify Piled server to close specific socket
			server.send(req.socket, ZERO_BYTES);
		}
		if (monitorSupported && monitors.size() > 0) {
			try {
				workerPool.execute(new Runnable() {
					@Override
					public void run() {
						if (PiledConfig.remoteMonitorSupported && monitors != null) {
							for (IRequestMonitor monitor : monitors) {
								monitor.requestClosedByRemote();
							}
						}
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
				// UNSAFE: worker may be frozen, unless we comment out heavy tasks
				/*
				if (monitorSupported && monitors.size() > 0) {
					for (IRequestMonitor monitor : monitors) {
						// monitor.requestClosedByRemote(); // might be heavy task
					}
				}
				// */
			}
		}
	}
	
	/*
	 * Update on response sent or response socket has been closed.
	 */
	protected void updateRequestResponse(ServerDataEvent dataEvent) {
		long now = System.currentTimeMillis();
		if (dataEvent.count != 0) {
			// notify request that bytes have been sent
			HttpRequest req = requests.get(dataEvent.socket);
			if (req == null) {
				req = closeSockets.get(dataEvent.socket);
			}
			if (req == null) {
				return;
			}
			req.lastSent = now;
			if (dataEvent.count > 0) {
				req.sending -= dataEvent.count;
				return;
			} else {
				req.sending -= -dataEvent.count;
				if (!(req.keepAliveMax == 0 && req.keepAliveTimeout == 0)) { // not "Connection: close"
					return;
				} else if (!req.done) { // HTTP 1.0
					// Server big file will be split into several packets for sending. If one packet
					// is sent, it will be notified that this packet is sent completed, but there are
					// chances that other packets are not being put into queue yet. Wait until req.done
					// is true before notifying that HTTP 1.0 request to close.
					return;
				} else if (req.sending <= 0) { // only after given HTTP 1.0 response is finished.
					// All data is sent and can be closed, tell server to close socket
					server.send(req.socket, ZERO_BYTES);
					// Continue rest lines to close this connection
				}
			}
		}
		
		// else dataEvent.count == 0
		// specific socket requests are being closed and should be removed.
		
		// there are no needs to close socket physically, Piled server is just
		// notifying worker that this socket is being closed physically on its
		// side.
		
		String pipeKey = null;
		HttpRequest req = requests.remove(dataEvent.socket);
		boolean removedFromCloseSockets = false;
		if (req == null) {
			req = closeSockets.remove(dataEvent.socket);
			removedFromCloseSockets = true;
		}
		if (req != null) {
			req.closed = now;
			if (!removedFromCloseSockets) {
				closeSockets.remove(dataEvent.socket);
			}
		}
		final IRequestMonitor monitor = (PiledConfig.remoteMonitorSupported
				&& req != null && !req.done && req.monitor != null) ? req.monitor : null;
		if (monitor != null || pipeKey != null) {
			//final String key = pipeKey; 
			try {
				workerPool.execute(new Runnable() {
					@Override
					public void run() {
						if (monitor != null) {
							monitor.requestClosedByRemote(); // might be a heavy task
						}
					}
				});
			} catch (Throwable e) {
				e.printStackTrace();
				// UNSAFE: worker may be frozen, unless we comment out heavy tasks
				if (monitor != null) {
					// monitor.requestClosedByRemote(); // might be a heavy task
				}
			}
		}
	}
	
	protected HttpRequest makeHttpRequest() {
		return new HttpRequest();
	}
	
	@Override
	public void run() {
		ServerDataEvent dataEvent;
		
		while (server == null || server.isRunning()) {
			// Wait for data to become available
			synchronized(queue) {
				while(queue.isEmpty()) {
					try {
						queue.wait();
					} catch (InterruptedException e) {
					}
					if (!(server == null || server.isRunning())) {
						return;
					}
				}
				dataEvent = (ServerDataEvent) queue.remove(0);
			}
			dataEvent.worker = this;
			
			if (dataEvent.socket == null) {
				// remove expired idled requests every 10s
				checkExpiredRequests();
				continue;
			}
			
			if (dataEvent.data == null || dataEvent.data.length == 0) {
				// notified by Piled server, no need to notify back Piled server.
				updateRequestResponse(dataEvent);
				continue;
			}
			
			HttpRequest request = requests.get(dataEvent.socket);
			/*
			if (request != null && request.keepAliveMax > 1 && request.requestCount >= request.keepAliveMax) {
				// remove HTTP request that request count is greater than max request count
				// Question: Simply remove socket and then add another request for the same socket?
				// OK
				requests.remove(request.socket);
				closeSockets.remove(request.socket);
				//closeSockets.put(request.socket, request);
				request = null;
			}
			//*/

			HttpRequest newReq = null;
			if (request == null) {
				request = makeHttpRequest();
				request.socket = dataEvent.socket;
				request.created = System.currentTimeMillis();
				newReq = request;
			} else {
				// No needs to find the last pipelining node				
				if (request.fullRequest) {
					if (!request.done) { // not yet finish responding, HTTP pipelining
						HttpRequest next = makeHttpRequest();
						Object mutex = request.mutex;
						if (request.prev == null && mutex == null) {
							// root HTTP request
							mutex = request.mutex = new Object();
						}
						
						next.userAgent = request.userAgent;
						next.remoteIP = request.remoteIP;
						next.created = System.currentTimeMillis();
						next.socket = request.socket;
						
						if (mutex == null) {
							mutex = new Object();
						}
						synchronized (mutex) {
							if (!request.done) {
								next.mutex = mutex;
								next.prev = request;
								request.next = next;
								request.mutex = mutex;
								
								request = next;
								newReq = next; // newReq will replace old cached request
							} else {
								if (request.prev == null) {
									request.mutex = null; // release mutex
								}
								// normal HTTP request: Request->Response->Request->Response->...
								// reset request as a new request
								request.reset();
							}
						}
					} else {
						// normal HTTP request: Request->Response->Request->Response->...
						// reset request as a new request
						request.reset();
					}
				} // else continue to parse left data to make a full request
			}
			if (request.remoteIP == null) { // prepare remote IP for request
				try {
					request.remoteIP = getSocketIP(dataEvent.socket.socket());
				} catch (Throwable e1) {
					e1.printStackTrace();
				}
				/*
				boolean blocked = false;
				String[] blockedIPs = PiledConfig.blockedIPs;
				if (blockedIPs != null && request.remoteIP != null) {
					for (String ip : blockedIPs) {
						if (ip != null && ip.length() > 0
								&& request.remoteIP.startsWith(ip)) {
							blocked = true;
							break;
						}
					}
				}
				if (blocked) {
					request.comet = false;
					request.done = true;
					request.created = 0;
					continue;
				}
				// */
			}
			//System.out.println("Parsing:" + new String(dataEvent.data));
			HttpQuickResponse resp = null;
			try {
				request.keepRawData = PiledConfig.proxyPilet != null;
				long now = System.currentTimeMillis();
				if (request.lastSent < 0 || now - Math.max(request.created, request.lastSent) < 10000) {
					request.lastSent = now;
				}
				resp = request.parseData(dataEvent.data);
			} catch (Throwable e) {
				System.out.println("Data: " + new String(dataEvent.data));
				e.printStackTrace();
				HttpResponse response = new HttpResponse();
				response.socket = dataEvent.socket;
				response.worker = dataEvent.worker;
				HttpWorkerUtils.send400Response(request, response);
				HttpLoggingUtils.addLogging(request.host, request, 400, 0);
				continue;
			}
			if (resp.code == 100) {
				if (newReq != null) {
					requests.put(dataEvent.socket, newReq);
				}
				continue;
			}
			request.fullRequest = true;
			request.created = System.currentTimeMillis();
			if (request.pending != null) { // HTTP pipeline streaming
				// constructing HTTP request chain
				HttpRequest req = request;
				HttpRequest next = null;
				boolean moreRequests = false;
				int pipelineRequests = 0;
				do {
					next = makeHttpRequest();
					Object mutex = req.mutex;
					if (req.prev == null && mutex == null) {
						// root HTTP request
						mutex = req.mutex = new Object();
					}

					next.userAgent = req.userAgent;
					next.remoteIP = req.remoteIP;
					next.created = req.created;
					next.socket = req.socket;
					next.pending = req.pending;
					next.dataLength = req.dataLength;
					
					req.pending = null;
					req.dataLength = 0;

					synchronized (mutex) {
						if (!req.done) {
							req.mutex = mutex;
							next.mutex = mutex;
							next.prev = req;
							req.next = next;
						} else {
							if (req.prev == null) {
								req.mutex = null;
							}
						}
					}
					
					req = next;
					HttpQuickResponse response = null;
					try {
						response = next.parseData(null);
					} catch (Throwable e) {
						System.out.println("Error Data: " + new String(dataEvent.data));
						e.printStackTrace();
						// One request in pipelining requests contains error!
						// Should finish earlier requests and send this 400 response
						next.response = null;
					}
					moreRequests = false;
					if (response != null && response.code >= 200) {
						next.fullRequest = true;
						pipelineRequests++;
						moreRequests = next.pending != null && pipelineRequests < PiledConfig.maxPipelineRequests;
					}
				} while (moreRequests);
				
				newReq = next; // next will replace existed request on give socket
			} // End of pending not null: HTTP pipeline streaming
			
			if (newReq != null) {
				requests.put(dataEvent.socket, newReq);
			}

			// Check to see if we need to make response.
			Object mutex = request.mutex;
			if (mutex != null) {
				synchronized (mutex) {
					if (request.prev != null) { // request.prev.done == false
						// skip makeResponse and leave it for previous request call back
						continue;
					}
				}
			}
			HttpResponse response = new HttpResponse();
			response.socket = dataEvent.socket;
			response.worker = dataEvent.worker;
			makeResponse(request, response, true);
		} // end of while loop
		
		workerPool.shutdown();
	}

	private String getSocketIP(Socket socket) {
		String ip = null;
		if (socket != null) {
			SocketAddress addr = socket.getRemoteSocketAddress();
			if (addr != null) {
				ip = addr.toString();
			}
		}
		if (ip != null) {
			if (ip.startsWith("/")) {
				ip = ip.substring(1);
			}
			int index = ip.lastIndexOf(':');
			if (index != -1) {
				ip = ip.substring(0, index);
				int idx = ip.indexOf('%');
				if (idx != -1) {
					ip = ip.substring(0, idx);
				}
			}
		}
		return ip;
	}

	@Override
	public void poolingRequest(SocketChannel socket, HttpRequest request) {
		closeSockets.put(socket, request);
	}
	
	private boolean passThroughFilters(HttpRequest request, HttpResponse response) {
		if (server.allFilters == null) {
			return false;
		}
		for (IFilter filter : server.allFilters) {
			try {
				if (filter.filter(request, response)) {
					return true;
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	void piletResponse(final HttpRequest req, final HttpResponse rsp) {
		boolean responsed = false;
		if (server.allPilets != null) {
			for (IPilet pilet : server.allPilets) {
				try {
					if (pilet instanceof ICloneablePilet) {
						ICloneablePilet cPilet = (ICloneablePilet) pilet;
						IPilet p = cPilet.clonePilet();
						if (p != null) {
							pilet = p;
						}
					}
					if (pilet instanceof IRequestMonitor) {
						req.monitor = (IRequestMonitor) pilet;
					}
					responsed = pilet.service(req, rsp);
					if (!responsed || !req.comet) {
						req.monitor = null;
					}
				} catch (Throwable e) {
					e.printStackTrace();
					HttpWorkerUtils.send500Response(req, rsp);
					HttpLoggingUtils.addLogging(req.host, req, 500, 0);
					responsed = true;
					break;
				}
				if (responsed) {
					break;
				}
			}
		}
		if (!responsed && server.resPilet != null) {
			try {
				if (server.resPilet instanceof IRequestMonitor) {
					req.monitor = (IRequestMonitor) server.resPilet;
				}
				responsed = server.resPilet.service(req, rsp);
				if (!responsed || !req.comet) {
					req.monitor = null;
				}
			} catch (Throwable e) {
				e.printStackTrace();
				HttpWorkerUtils.send500Response(req, rsp);
				HttpLoggingUtils.addLogging(req.host, req, 500, 0);
				responsed = true;
			}
		}
		if (!responsed) {
			HttpWorkerUtils.send404NotFound(req, rsp);
			HttpLoggingUtils.addLogging(req.host, req, 404, 0);
		}
	}
	
	protected void respondRequestWithData(HttpRequest req, HttpResponse rsp) {
		// normal requests with query
		piletResponse(req, rsp);
	}
	
	protected void makeResponse(HttpRequest request, HttpResponse response, boolean checkingProxyPilet) {
		String proxyPilet = PiledConfig.proxyPilet;
		if (checkingProxyPilet && proxyPilet != null) {
			if (server.proxyPilet == null) {
				IPilet p = server.loadPilet(proxyPilet);
				if (p != null) {
					server.proxyPilet = p;
				}
			}
			if (server.proxyPilet != null) {
				final HttpRequest req = request;
				final HttpResponse rsp = response;
				try {
					workerPool.execute(new Runnable() {
						
						@Override
						public void run() {
							boolean responsed = false;
							try {
								IPilet pilet = server.proxyPilet;
								if (pilet instanceof ICloneablePilet) {
									ICloneablePilet cPilet = (ICloneablePilet) pilet;
									IPilet p = cPilet.clonePilet();
									if (p != null) {
										pilet = p;
									}
								}
								if (pilet instanceof IRequestMonitor) {
									req.monitor = (IRequestMonitor) pilet;
								}
								responsed = pilet.service(req, rsp);
								if (!responsed || !req.comet) {
									req.monitor = null;
								}
							} catch (Throwable e) {
								e.printStackTrace();
								HttpWorkerUtils.send500Response(req, rsp);
								HttpLoggingUtils.addLogging(req.host, req, 500, 0);
								responsed = true;
							}
							if (!responsed) {
								makeResponse(req, rsp, false);
							} else {
								chainingRequest(req, rsp);
							}
						}
						
					});
				} catch (Throwable e) {
					e.printStackTrace();
					HttpWorkerUtils.send500Response(req, rsp);
					HttpLoggingUtils.addLogging(req.host, req, 500, 0);
					chainingRequest(req, rsp);
				}
				return;
			}
		}
		HttpQuickResponse resp = request.response;
		if (resp == null) {
			HttpWorkerUtils.send404NotFound(request, response);
			HttpLoggingUtils.addLogging(request.host, request, 404, 0);
			chainingRequest(request, response);
			return;
		}
		if (resp.code == 200) {
			//request.created = System.currentTimeMillis();
			if (((resp.contentType == null || resp.content == null) && request.requestData == null)
					|| (!"GET".equals(request.method) && !"POST".equals(request.method))) {
				if (passThroughFilters(request, response)) {
					chainingRequest(request, response);
					return;
				}
				final HttpRequest req = request;
				final HttpResponse rsp = response;
				try {
					workerPool.execute(new Runnable() {
						
						@Override
						public void run() {
							piletResponse(req, rsp);
							chainingRequest(req, rsp);
						}
					});
				} catch (Throwable e) {
					e.printStackTrace();
					HttpWorkerUtils.send500Response(req, rsp);
					HttpLoggingUtils.addLogging(req.host, req, 500, 0);
					chainingRequest(req, rsp);
				}
			} else if (request.requestData != null) {
				if (passThroughFilters(request, response)) {
					chainingRequest(request, response);
					return;
				}
				final HttpRequest req = request;
				final HttpResponse rsp = response;
				try {
					workerPool.execute(new Runnable() {
						
						@Override
						public void run() {
							respondRequestWithData(req, rsp);
							chainingRequest(req, rsp);
						}
						
					});
				} catch (Throwable e) {
					e.printStackTrace();
					HttpWorkerUtils.send500Response(req, rsp);
					HttpLoggingUtils.addLogging(req.host, req, 500, 0);
					chainingRequest(req, rsp);
				}
			} else { // Simple RPC or Simple Pipe output, already in ascii-127 format
//				System.out.println("Response: " + request.url);
				StringBuilder responseBuilder = new StringBuilder(128);
				responseBuilder.append("HTTP/1.");
				responseBuilder.append(request.v11 ? '1' : '0');
				responseBuilder.append(" 200 OK\r\n");
				String serverName = HttpConfig.serverSignature;
				if (request.requestCount < 1 && serverName != null && serverName.length() > 0) {
					responseBuilder.append("Server: ").append(serverName).append("\r\n");
				}
				boolean closeSocket = HttpWorkerUtils.checkKeepAliveHeader(request, responseBuilder);
				byte[] bytes = resp.content.getBytes();
				int length = bytes.length;
				boolean toGZip = length > HttpConfig.gzipStartingSize && request.supportGZip && HttpWorkerUtils.isUserAgentSupportGZip(request.userAgent);
				if (toGZip) {
					bytes = HttpWorkerUtils.gZipCompress(bytes);
					length = bytes.length;
					responseBuilder.append("Content-Encoding: gzip\r\n");
				}
				responseBuilder.append("Content-Type: ");
				responseBuilder.append(resp.contentType);
				responseBuilder.append("\r\nContent-Length: ");
				responseBuilder.append(length);
				responseBuilder.append("\r\n\r\n");
				byte[] responseBytes = responseBuilder.toString().getBytes();
				byte[] outBytes = new byte[responseBytes.length + bytes.length];
				System.arraycopy(responseBytes, 0, outBytes, 0, responseBytes.length);
				System.arraycopy(bytes, 0, outBytes, responseBytes.length, bytes.length);
				request.sending = outBytes.length;
				server.send(response.socket, outBytes);
				if (closeSocket) {
					closeSockets.put(response.socket, request);
				}
				//request.done = true;
				chainingRequest(request, response);
			}
		} else { // if (resp.code / 100 == 4) {
			errorRequests++;
			if (request.userAgent == null || request.userAgent.indexOf("Infoaxe") == -1) {
				System.out.println("Bad HTTP Request: " + resp.code);
				request.debugPrint();
			}
//			System.out.println("Response: " + request.url);
			
			HttpWorkerUtils.send400Response(request, response);
			HttpLoggingUtils.addLogging(request.host, request, 400, 0);
			chainingRequest(request, response);
		//} else { // other resp.code ?...
			//System.out.println("Not supported");
		}
	}

	/*
	 * Normal requests have already been answered. Comet requests are already
	 * in queue. Try to check requests in chain.
	 */
	@Override
	public void chainingRequest(HttpRequest req, HttpResponse resp) {
		if (req.comet) {
			return; // Still in comet queue
		}
		
		// normal requests
		totalRequests++;
		req.requestCount++;
		
		Object mutex = req.mutex;
		if (mutex != null) {
			boolean toResponse = false;
			HttpRequest next = null;
			synchronized (mutex) {
				// Discard any requests link in field prev.
				if (req.prev != null) {
					// break it down previous request chaining
					req.prev.next = null;
					req.prev = null;
				}
				next = req.next;
				if (next == null) {
					req.mutex = null;
				} else {
					// break it down following request chaining
					req.next = null;
					next.prev = null;
				}
				toResponse = next != null && next.fullRequest && !req.done;
				req.done = true;
			}
			// Begin to deal next request
			if (next != null) {
				next.requestCount = req.requestCount;
				if (toResponse) {
					makeResponse(next, resp, true);
				}
				// else if next is not full request, wait until become full request
				// else if already done, response are being made to next request from other thread  
			} else if (req.keepAliveMax <= 0) {
				// request should be closed, try to close it
				removeSocket(req.socket);
				server.send(req.socket, ZERO_BYTES);
			} // else wait until requests come.
		} else { // has no previous or next requests
			req.done = true;
			if (req.keepAliveMax <= 0) {
				// request should be closed, try to close it
				removeSocket(req.socket);
				server.send(req.socket, ZERO_BYTES);
			} // else wait until requests come.
		}
	}

}
