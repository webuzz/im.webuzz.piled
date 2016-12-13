package im.webuzz.piled;

abstract class CometRunnable implements Runnable {

	public boolean called = false;
	public boolean headerSent = false;
	public boolean closeSocket = false;
	
}
