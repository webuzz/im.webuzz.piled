/*******************************************************************************
 * Copyright (c) 2010 - 2011 webuzz.im
 *
 * Author:
 *   Zhou Renjian / zhourenjian@gmail.com - initial API and implementation
 *******************************************************************************/

package im.webuzz.piled;

import im.webuzz.pilet.HttpRequest;
import im.webuzz.pilet.IFilter;
import im.webuzz.pilet.IPiledServer;
import im.webuzz.pilet.IPiledWrapping;
import im.webuzz.pilet.IPilet;
import im.webuzz.pilet.IServerBinding;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public abstract class PiledAbstractServer implements IPiledServer {
	
	private static List<PiledAbstractServer> allServers = Collections.synchronizedList(new ArrayList<PiledAbstractServer>());
	
	// The host:port combination to listen on
	InetAddress hostAddress;
	int port;

	// The channel on which we'll accept connections
	ServerSocketChannel serverChannel;

	// The selector we'll be monitoring
	Selector selector;

	HttpWorker[] workers;

	// A queue of PendingChange instances
	Queue<ChangeRequest> pendingChanges = new ConcurrentLinkedQueue<ChangeRequest>();

	// Maps a SocketChannel to a list of ByteBuffer instances
	Map<SocketChannel, List<ByteBuffer>> pendingData = null;

	private Set<SelectionKey> lostKeys2m;
	
	private Set<SelectionKey> lostKeys1m;
	
	Object closingMutex = new Object();
	Map<SocketChannel, Socket> closingSockets = new ConcurrentHashMap<SocketChannel, Socket>();
	
	long processing = 0;
	
	private boolean sslEnabled;
	
	boolean running;
	
	private IPiledWrapping[] allWrappers;
	IFilter[] allFilters;
	IPilet[] allPilets;
	IPilet resPilet;
	IPilet proxyPilet;

	private ClassLoader simpleClassLoader = null;
	
	private Set<String> reloadingClasses = new ConcurrentSkipListSet<String>();
	
	public PiledAbstractServer(InetAddress hostAddress, int port, boolean sslEnabled, HttpWorker[] workers) {
		this.running = true;
		this.hostAddress = hostAddress;
		this.port = port;
		this.sslEnabled = sslEnabled;
		this.pendingData = new HashMap<SocketChannel, List<ByteBuffer>>();
		String staticResoucePilet = PiledConfig.staticResoucePilet;
		if (staticResoucePilet != null && staticResoucePilet.length() > 0) {
			try {
				//Class<?> clazz = Class.forName(staticResoucePilet);
				//Object inst = clazz.newInstance();
				Object inst = loadSimpleInstance(staticResoucePilet);
				if (inst instanceof IPilet) {
					resPilet = (IPilet) inst;
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		allServers.add(this);
		
		bindWorkers(workers);
	}

	protected void bindWorkers(HttpWorker[] workers) {
		this.workers = workers;
		if (this.workers != null) {
			for (int i = 0; i < workers.length; i++) {
				this.workers[i].bindingServer(this, null);
			}
		}
	}
	
	public void init() throws IOException {
		this.selector = this.initSelector();
	}

	public void send(SocketChannel socket, byte[] data) {
		send(socket, data, 0, data != null ? data.length : 0);
	}
	
	public void send(SocketChannel socket, byte[] data, int offset, int length) {
		if (socket == null) {
			if (!(data == null && length == -1)) {
				return;
			}
		}
		ChangeRequest r = null;
		if (data == null && length == -1) {
			// Indicate we want server to clean expired or hanged connections
			r = new ChangeRequest(null, ChangeRequest.CLEAN, -1);
		} else {
			// Indicate we want the interest ops set changed
			r = new ChangeRequest(socket, ChangeRequest.CHANGEOPS, SelectionKey.OP_WRITE, ByteBuffer.wrap(data, offset, length));
		}
		pendingChanges.offer(r); // ignore return value, as pendingChagnes is a linked queue
		
		// Finally, wake up our selecting thread so it can make the required changes
		this.selector.wakeup();
	}

	@Override
	public Selector getSelector() {
		return selector;
	}
	
	public void runLoop() throws IOException {
		// Register the server socket channel, indicating an interest in 
		// accepting new connections
		serverChannel.register(selector, SelectionKey.OP_ACCEPT);

		running = true;
		lostKeys2m = new HashSet<SelectionKey>();
		lostKeys1m = new HashSet<SelectionKey>();
		
		while (running) {
			try {
				processChangeRequests();
				processing++;
				processNetworkEvents();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
		System.out.println("[!!!!!!!!!!!!!!!]");
		System.out.println("Server is closing down!");
		System.out.println("[***************]");
		serverChannel.close();
	}

	private void processChangeRequests() {
		// Process any pending changes
		ChangeRequest change = null;
		while ((change = pendingChanges.poll()) != null) {
			if (change.socket == null && change.type != ChangeRequest.CLEAN) {
				System.out.println("Piled: Socket is null!!!");
				continue;
			}
			try {
				SelectionKey key = null;
				switch (change.type) {
				case ChangeRequest.CHANGEOPS:
					key = change.socket.keyFor(this.selector);
					if (change.data != null) {
						// And queue the data we want written
						List<ByteBuffer> queue = (List<ByteBuffer>) this.pendingData.get(change.socket);
						if (change.data.remaining() == 0 && change.ops == SelectionKey.OP_WRITE
								&& (queue == null || queue.isEmpty())) {
							// to close socket
							closeChannel(key, change.socket, true); // confirm closing socket
							break;
						}
						if (queue == null) {
							queue = new LinkedList<ByteBuffer>();
							this.pendingData.put(change.socket, queue);
						}
						queue.add(change.data);
					}
					if (key != null && key.isValid()) {
						//System.out.println("Set " + key + " ops = " + change.ops);
						ChangeRequest top = null;
						while ((top = pendingChanges.peek()) != null) {
							if (top.type == ChangeRequest.CHANGEOPS && top.ops == change.ops
									&& top.data == null && top.socket == change.socket) {
								//System.out.println("Skip " + top.socket + " " + top.ops);
								pendingChanges.poll(); // skip next
								continue;
							}
							break;
						}
						key.interestOps(change.ops);
					} else {
						closeChannel(key, change.socket, false);
					}
					break;
				case ChangeRequest.CLEAN:
					this.cleanExpiredHangedConnections();
					break;
				case ChangeRequest.CLOSE:
					//key = change.socket.keyFor(this.selector);
					break;
				default:
				}
			} catch (Throwable e) {
				e.printStackTrace();
				closeChannel(change.socket.keyFor(selector), change.socket, false);
			}
		}
	}

	private void processNetworkEvents() throws IOException {
		// Wait for an event one of the registered channels
		/*int ns = */this.selector.select();
		Set<SelectionKey> readyKeys = this.selector.selectedKeys();

		// Iterate over the set of keys for which events are available
		Iterator<SelectionKey> selectedKeys = readyKeys.iterator();
		while (selectedKeys.hasNext()) {
			SelectionKey key = (SelectionKey) selectedKeys.next();
			selectedKeys.remove();

			if (!key.isValid()) {
				closeChannel(key, key.channel(), false);
				continue;
			}

			try {
				// Check what event is available and deal with it
				if (key.isAcceptable()) {
					this.accept(key);
				} else if (key.isReadable()) {
					this.read(key);
				} else if (key.isWritable()) {
					this.write(key);
				}
			} catch (Throwable e) {
				e.printStackTrace();
				closeChannel(key, key.channel(), false);
			}
		}
	}

	protected void closeExtraResource(SocketChannel socketChannel, boolean notifyClosing) {
		// To be override
	}
	
	void closeChannel(SelectionKey key, SelectableChannel channel, boolean confirm) {
		closeChannel(key, channel, confirm, false);
	}
	
	void closeChannel(SelectionKey key, SelectableChannel channel, boolean confirm, boolean inAnotherthread) {
		boolean notifyClosing = confirm; // set initial value: if socket closing is confirm
		if (channel != null) {
			if (channel instanceof SocketChannel) {
				pendingData.remove(channel);
				SocketChannel socketChannel = (SocketChannel) channel;
				workers[channel.hashCode() % this.workers.length].removeSocket(socketChannel);
				Socket socket = socketChannel.socket();
				if (socket != null) {
					closingSockets.put(socketChannel, socket);
					notifyClosing = true;
				}
				closeExtraResource(socketChannel, notifyClosing);
			} else {
				if (channel instanceof ServerSocketChannel) {
					System.out.println("Server socket is being closed?");
					new RuntimeException("Server socket being closed").printStackTrace();
					return;
				}
			}
		}
		
		if (inAnotherthread) {
			if (channel instanceof SocketChannel) {
				pendingChanges.offer(new ChangeRequest((SocketChannel) channel, ChangeRequest.CLOSE));
			}
		} else {
			if (key != null) {
				try {
					key.cancel();
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}
			if (channel != null) {
				try {
					channel.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		if (notifyClosing) {
			synchronized (closingMutex) {
				closingMutex.notify();
			}
		}
	}

	protected abstract void write(SelectionKey key) throws IOException;

	protected abstract void read(SelectionKey key) throws IOException;

	protected abstract void accept(SelectionKey key) throws IOException;

	protected void cleanExpiredHangedConnections() {
		boolean notifyClosing = false;
		Set<SelectionKey> possibleLostKeys = new HashSet<SelectionKey>();
		try {
			Set<SelectionKey> keys = selector.keys();
			for (SelectionKey k : keys) {
				// ServerSocketChannel
				if (k.isValid() && k.interestOps() == SelectionKey.OP_ACCEPT) {
					continue;
				}
				SelectableChannel channel = k.channel();
				//if (channel instanceof ServerSocketChannel) {
				//	System.out.println("Should never reach here?!");
				//	continue;
				//}
				if (!workers[channel.hashCode() % workers.length].requests.containsKey(channel)) {
					possibleLostKeys.add(k);
				} else {
					lostKeys2m.remove(k);
					lostKeys1m.remove(k);
				}
			}
			int lostValidKeys = 0;
			for (SelectionKey k : lostKeys2m) {
				if (!possibleLostKeys.contains(k)) {
					continue; // already closed
				}
				if (k.isValid()) {
					lostValidKeys++;
					/*
					System.out.println("To close " + k.interestOps() + " | " + k.readyOps()
							+ " | " + k.isAcceptable()
							+ " | " + k.isConnectable()
							+ " | " + k.isReadable()
							+ " | " + k.isWritable()
							+ " | " + k.isValid() + " // " + k + " " + k.channel());
					// */
				} else {
					System.out.println("Invalid key " + k);
				}
				SocketChannel channel = (SocketChannel) k.channel();
				if (channel != null) {
					Socket socket = channel.socket();
					if (socket != null) {
						closingSockets.put(channel, socket);
						notifyClosing = true;
					}
					pendingData.remove(channel);
					closeExtraResource(channel, notifyClosing);
				}
				try {
					k.cancel();
				} catch (Throwable e) {
					e.printStackTrace();
				}
				if (channel != null) {
					try {
						channel.close();
					} catch (Throwable e) {
						e.printStackTrace();
					}
				}
			}
			if (lostValidKeys > 0) {
				System.out.println("Lost " + lostValidKeys + " connections.");
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
		
		lostKeys2m.clear();
		for (SelectionKey k : lostKeys1m) {
			if (!possibleLostKeys.contains(k)) {
				continue; // already closed
			}
			lostKeys2m.add(k);
		}
		possibleLostKeys.removeAll(lostKeys1m);
		lostKeys1m = possibleLostKeys;
		
		if (notifyClosing) {
			synchronized (closingMutex) {
				closingMutex.notify();
			}
		}
		int size2m = lostKeys2m.size();
		if (size2m != 0) {
			int size1m = lostKeys1m.size();
			System.out.println("Lost key set " + size1m + " / " + size2m + " @ " + new Date());
		}
	}

	protected Selector initSelector() throws IOException {
		// Create a new selector
		Selector socketSelector = SelectorProvider.provider().openSelector();

		// Create a new non-blocking server socket channel
		this.serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

		ServerSocket socket = serverChannel.socket();
		//socket.setReceiveBufferSize(8096);
		//socket.setPerformancePreferences(0, 1, 2);
		socket.setReuseAddress(true);
		// Bind the server socket to the specified address and port
		socket.bind(new InetSocketAddress(this.hostAddress, this.port), PiledConfig.socketBacklog);

		return socketSelector;
	}

	void initializeWrapperInstances() {
		List<IPiledWrapping> allWrappings = new LinkedList<IPiledWrapping>();
		Set<String> allWrapperNames = new HashSet<String>();
		Map<String, IPiledWrapping> oldWrappers = new HashMap<String, IPiledWrapping>();
		if (allWrappers != null) {
			// prepare existed wrappers into set and map
			for (IPiledWrapping wrapping : allWrappers) {
				if (wrapping != null) {
					String clazzName = wrapping.getClass().getName();
					oldWrappers.put(clazzName, wrapping);
					allWrapperNames.add(clazzName);
				}
			}
		}
		String[] wrappers = PiledConfig.wrappers;
		if (wrappers != null) {
			for (String wrapper : wrappers) {
				if (wrapper == null) {
					continue;
				}
				wrapper = wrapper.trim();
				if (wrapper.length() <= 0) {
					continue;
				}
				if (allWrapperNames.contains(wrapper)) {
					// Check if existed in old wrappers, try to reuse it
					IPiledWrapping wrapping = oldWrappers.get(wrapper);
					if (wrapping != null) {
						addWrapper2List(wrapper, wrapping, allWrappings);
						// Remove from old wrappers to avoid adding it again
						oldWrappers.remove(wrapper);
					}
					continue;
				}
				try {
					//Class<?> clazz = Class.forName(wrapper);
					//Object inst = clazz.newInstance();
					Object inst = loadSimpleInstance(wrapper);
					if (inst instanceof IPiledWrapping) {
						IPiledWrapping wrapping = (IPiledWrapping) inst;
						if (wrapping instanceof IServerBinding) {
							IServerBinding binding = (IServerBinding) wrapping;
							binding.binding(this);
						}
						wrapping.beforeStartup(this);
						allWrappings.add(wrapping);
						allWrapperNames.add(wrapper);
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
		
		if (resPilet instanceof IPiledWrapping && allWrapperNames.contains(resPilet.getClass().getName())) {
			allWrappings.add((IPiledWrapping) resPilet);
		}
		if (proxyPilet instanceof IPiledWrapping && allWrapperNames.contains(proxyPilet.getClass().getName())) {
			allWrappings.add((IPiledWrapping) proxyPilet);
		}
		allWrappers = allWrappings.toArray(new IPiledWrapping[allWrappings.size()]);
	}

	private void addWrapper2List(String wrapper, IPiledWrapping wrapping,
			List<IPiledWrapping> allWrappings) {
		if (reloadingClasses.contains(wrapper)) {
			// need to reload wrapper class
			Object inst = loadSimpleInstance(wrapper);
			if (inst instanceof IPiledWrapping) {
				wrapping = (IPiledWrapping) inst;
				if (wrapping instanceof IServerBinding) {
					IServerBinding binding = (IServerBinding) wrapping;
					binding.binding(this);
				}
				wrapping.beforeStartup(this);
				allWrappings.add(wrapping);
				// remove reloading to avoid calling #beforeStartup again 
				reloadingClasses.remove(wrapper);
			} // else do nothing
		} else {
			allWrappings.add(wrapping);
		}
	}

	void closeWrapperInstances() {
		if (allWrappers != null) {
			for (IPiledWrapping wrapping : allWrappers) {
				try {
					wrapping.afterClosed(this);
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	void initializeFilterInstances() {
		List<IFilter> allFilterList = new LinkedList<IFilter>();
		Set<String> allFilterNames = new HashSet<String>();
		Map<String, IFilter> oldFilters = new HashMap<String, IFilter>();
		Map<String, IFilter> wrapperFilters = new HashMap<String, IFilter>();
		if (allFilters != null) {
			// prepare existed filters into set and map
			for (IFilter f : allFilters) {
				if (f != null) {
					String filter = f.getClass().getName();
					oldFilters.put(filter, f);
					allFilterNames.add(filter);
				}
			}
		}
		if (allWrappers != null) {
			// prepare existed filters from wrappers into map
			for (IPiledWrapping wrapper : allWrappers) {
				if (wrapper != null && wrapper instanceof IFilter) {
					wrapperFilters.put(wrapper.getClass().getName(), (IFilter) wrapper);
				}
			}
		}
		String[] filters = PiledConfig.filters;
		if (filters != null) {
			for (String filter : filters) {
				if (filter == null) {
					continue;
				}
				filter = filter.trim();
				if (filter.length() <= 0) {
					continue;
				}
				if (allFilterNames.contains(filter)) {
					// already existed, considered existed into previous versions
					IFilter f = oldFilters.get(filter);
					if (f != null) {
						addFilter2List(filter, f, allFilterList);
						// Remove from old filters to avoid adding it again
						oldFilters.remove(filter);
					}
					continue;
				}
				IFilter wrapperFilter = wrapperFilters.get(filter);
				if (wrapperFilter != null) {
					// Add filter from wrappers. No need to reload class, as wrappers
					// checks theirs reloading already. 
					allFilterList.add(wrapperFilter);
					allFilterNames.add(filter);
					wrapperFilters.remove(filter);
					continue;
				}
				try {
					//Class<?> clazz = Class.forName(filter);
					//Object inst = clazz.newInstance();
					Object inst = loadSimpleInstance(filter);
					if (inst instanceof IFilter) {
						IFilter f = (IFilter) inst;
						if (inst instanceof IServerBinding) {
							IServerBinding binding = (IServerBinding) inst;
							binding.binding(this);
						}
						allFilterList.add(f);
						allFilterNames.add(filter);
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
		if (allWrappers != null) {
			for (IPiledWrapping wrapper : allWrappers) {
				if (wrapper != null && wrapper instanceof IFilter) {
					IFilter f = (IFilter) wrapper;
					String filter = f.getClass().getName();
					if (allFilterNames.contains(filter)) {
						// already existed, considered existed into previous versions
						IFilter ff = oldFilters.get(filter);
						if (ff != null) {
							addFilter2List(filter, ff, allFilterList);
							// Remove from old filters to avoid adding it again
							oldFilters.remove(filter);
						}
						continue;
					}
					if (wrapperFilters.containsKey(filter)) {
						// if not contains, it has already been added
						allFilterList.add(f);
						allFilterNames.add(filter);
					}
				} // end if IFilter
			} // end of for
		} // end if allWrappers
		
		if (resPilet instanceof IFilter && allFilterNames.contains(resPilet.getClass().getName())) {
			allFilterList.add((IFilter) resPilet);
		}
		if (proxyPilet instanceof IFilter && allFilterNames.contains(proxyPilet.getClass().getName())) {
			allFilterList.add((IFilter) proxyPilet);
		}
		allFilters = allFilterList.toArray(new IFilter[allFilterList.size()]);
	}

	private void addFilter2List(String filter, IFilter f,
			List<IFilter> allFilterList) {
		if (reloadingClasses.contains(filter)) {
			// need reloading
			Object inst = loadSimpleInstance(filter);
			if (inst instanceof IFilter) {
				f = (IFilter) inst;
				if (inst instanceof IServerBinding) {
					IServerBinding binding = (IServerBinding) inst;
					binding.binding(this);
				}
				allFilterList.add(f);
				reloadingClasses.remove(filter);
			} // else do nothing
		} else {
			allFilterList.add(f);
		}
	}

	void initializePiletInstances() {
		List<IPilet> allPiletList = new LinkedList<IPilet>();
		Set<String> allPiletNames = new HashSet<String>();
		Map<String, IPilet> oldPilets = new HashMap<String, IPilet>();
		Map<String, IPilet> wrapperPilets = new HashMap<String, IPilet>();
		Map<String, IPilet> filterPilets = new HashMap<String, IPilet>();
		if (allPilets != null) {
			// prepare existed pilets into set and map
			for (IPilet p : allPilets) {
				if (p != null) {
					String pilet = p.getClass().getName();
					oldPilets.put(pilet, p);
					allPiletNames.add(pilet);
				}
			}
		}
		if (allWrappers != null) {
			// prepare existed pilets from wrappers into map
			for (IPiledWrapping wrapper : allWrappers) {
				if (wrapper != null && wrapper instanceof IPilet) {
					wrapperPilets.put(wrapper.getClass().getName(), (IPilet) wrapper);
				}
			}
		}
		if (allFilters != null) {
			// prepare existed pilets from filters into map
			for (IFilter filter : allFilters) {
				if (filter != null && filter instanceof IPilet) {
					filterPilets.put(filter.getClass().getName(), (IPilet) filter);
				}
			}
		}
		String[] pilets = PiledConfig.pilets;
		if (pilets != null) {
			for (String pilet : pilets) {
				if (pilet == null) {
					continue;
				}
				pilet = pilet.trim();
				if (pilet.length() <= 0) {
					continue;
				}
				if (allPiletNames.contains(pilet)) {
					// already existed, considered existed into previous versions
					IPilet p = oldPilets.get(pilet);
					if (p != null) {
						addPilet2List(pilet, p, allPiletList);
						// Remove from old pilets to avoid adding it again
						oldPilets.remove(pilet);
					}
					continue;
				}
				IPilet wrapperPilet = wrapperPilets.get(pilet);
				if (wrapperPilet != null) {
					// Add pilet from wrappers. No need to reload class, as wrappers
					// checks theirs reloading already. 
					allPiletList.add(wrapperPilet);
					allPiletNames.add(pilet);
					wrapperPilets.remove(pilet);
					continue;
				}
				IPilet filterPilet = filterPilets.get(pilet);
				if (filterPilet != null) {
					// Add pilet from filters. No need to reload class, as wrappers
					// checks theirs reloading already. 
					allPiletList.add(filterPilet);
					allPiletNames.add(pilet);
					filterPilets.remove(pilet);
					continue;
				}
				try {
					//Class<?> clazz = Class.forName(pilet);
					//Object inst = clazz.newInstance();
					Object inst = loadSimpleInstance(pilet);
					if (inst instanceof IPilet) {
						if (inst instanceof IServerBinding) {
							IServerBinding binding = (IServerBinding) inst;
							binding.binding(this);
						}
						IPilet p = (IPilet) inst;
						allPiletList.add(p);
						allPiletNames.add(pilet);
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}
		if (allWrappers != null) {
			// Add left pilets from wrappers to pilet list
			for (IPiledWrapping wrapper : allWrappers) {
				if (wrapper != null && wrapper instanceof IPilet) {
					IPilet p = (IPilet) wrapper;
					String pilet = p.getClass().getName();
					if (allPiletNames.contains(pilet)) {
						// already existed, considered existed into previous versions
						IPilet pp = oldPilets.get(pilet);
						if (pp != null) {
							addPilet2List(pilet, pp, allPiletList);
							// Remove from old pilets to avoid adding it again
							oldPilets.remove(pilet);
						}
						continue;
					}
					if (wrapperPilets.containsKey(pilet)) {
						// if not contains, it has already been added
						allPiletList.add(p);
						allPiletNames.add(pilet);
					}
				}
			}
		}
		if (allFilters != null) {
			// Add left pilets from filters to pilet list
			for (IFilter f : allFilters) {
				if (f != null && f instanceof IPilet) {
					IPilet p = (IPilet) f;
					String pilet = p.getClass().getName();
					if (allPiletNames.contains(pilet)) {
						// already existed, considered existed into previous versions
						IPilet pp = oldPilets.get(pilet);
						if (pp != null) {
							addPilet2List(pilet, pp, allPiletList);
							// Remove from old pilets to avoid adding it again
							oldPilets.remove(pilet);
						}
						continue;
					}
					if (filterPilets.containsKey(pilet)) {
						// if not contains, it has already been added
						allPiletList.add(p);
						allPiletNames.add(pilet);
					}
				}
			}
		}

		allPilets = allPiletList.toArray(new IPilet[allPiletList.size()]);
		
		String staticPiletName = PiledConfig.staticResoucePilet;
		String resPiletName = resPilet == null ? null : resPilet.getClass().getName();
		if ((staticPiletName == null || staticPiletName.length() == 0) && resPiletName != null) {
			resPilet = null;
		} else if (staticPiletName != null && !staticPiletName.equals(resPiletName)) {
			IPilet p = loadPilet(staticPiletName);
			if (p != null) {
				resPilet = p;
			}
		}
		String proxyPiletName = PiledConfig.proxyPilet;
		String prxPiletName = proxyPilet == null ? null : proxyPilet.getClass().getName();
		if ((proxyPiletName == null || proxyPiletName.length() == 0) && prxPiletName != null) {
			proxyPilet = null;
		} else if (proxyPiletName != null && !proxyPiletName.equals(prxPiletName)) {
			IPilet p = loadPilet(proxyPiletName);
			if (p != null) {
				proxyPilet = p;
			}
		}
	}

	private void addPilet2List(String pilet, IPilet p, List<IPilet> allPiletList) {
		if (reloadingClasses.contains(pilet)) {
			Object inst = loadSimpleInstance(pilet);
			if (inst instanceof IPilet) {
				p = (IPilet) inst;
				if (inst instanceof IServerBinding) {
					IServerBinding binding = (IServerBinding) inst;
					binding.binding(this);
				}
				allPiletList.add(p);
				reloadingClasses.remove(pilet);
			} // else do nothing
		} else {
			allPiletList.add(p);
		}
	}
	
	public IPilet loadPilet(String piletName) {
		try {
			//Class<?> clazz = Class.forName(piletName);
			//Object inst = clazz.newInstance();
			Object inst = loadSimpleInstance(piletName);
			if (inst instanceof IPilet) {
				if (inst instanceof IServerBinding) {
					IServerBinding binding = (IServerBinding) inst;
					binding.binding(this);
				}
				if (inst instanceof IPiledWrapping) {
					IPiledWrapping wrapping = (IPiledWrapping) inst;
					wrapping.beforeStartup(this);
				}
				return (IPilet) inst;
			}
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
	}

	public ClassLoader getSimpleClassLoader() {
		return simpleClassLoader;
	}

	public void setSimpleClassLoader(ClassLoader loader) {
		simpleClassLoader = loader;
	}

	private Object loadSimpleInstance(String clazzName) {
		try {
			Class<?> runnableClass = null;
			ClassLoader classLoader = simpleClassLoader;
			if (classLoader != null) {
				runnableClass = classLoader.loadClass(clazzName);
			} else {
				runnableClass = Class.forName(clazzName);
			}
			if (runnableClass != null) {
				return runnableClass.newInstance();
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}
		return null;
	}

	public void reloadClasses(String[] clazzNames, String path, String tag) {
		for (String name : clazzNames) {
			if (name != null && name.length() > 0) {
				reloadingClasses.add(name);
			}
		}
	}
	
	@Override
	public int getPort() {
		return port;
	}

	@Override
	public boolean isSSLEnabled() {
		return sslEnabled;
	}

	@Override
	public boolean isRunning() {
		return running;
	}
	
	@Override
	public void stop() {
		running = false;
		for (int i = 0; i < workers.length; i++) {
			synchronized (workers[i].queue) {
				workers[i].queue.notify();
			}
		}

		selector.wakeup();
		
		allServers.remove(this);
		
		synchronized (closingMutex) {
			closingMutex.notify();
		}
	}

	@Override
	public long getProcessingIOs() {
		return processing;
	}
	
	@Override
	public HttpRequest[] getActiveRequests() {
		List<HttpRequest> reqs = new LinkedList<HttpRequest>();
		for (int i = 0; i < workers.length; i++) {
			try {
				reqs.addAll(workers[i].requests.values());
			} catch (Exception e) {
				// e.printStackTrace();
				try {
					reqs.addAll(workers[i].requests.values());
				} catch (Exception e2) {
					// e2.printStackTrace();
				}
			}
		}
		return reqs.toArray(new HttpRequest[reqs.size()]);
	}

	@Override
	public long getErrorRequests() {
		long errReqs = 0;
		for (int i = 0; i < workers.length; i++) {
			errReqs += workers[i].errorRequests;
		}
		return errReqs;
	}

	@Override
	public long getTotalRequests() {
		long totalReqs = 0;
		for (int i = 0; i < workers.length; i++) {
			totalReqs += workers[i].totalRequests;
		}
		return totalReqs;
	}

	public IPiledServer[] getAllServers() {
		return allServers.toArray(new PiledAbstractServer[allServers.size()]);
	}
	
}
