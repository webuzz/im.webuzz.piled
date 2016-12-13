package im.webuzz.piled;

import java.lang.reflect.Method;

public class Start {

	public static void main(final String[] args) {
		if (args == null || args.length < 2) {
			System.out.println("Start <*.ini> <Main class>");
			return;
		}
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				PiledServer.main(new String[] { args[0] });
			}
			
		}, "Piled").start();
		
		try {
			Class<?> mainClass = (Class<?>) Class.forName(args[1]);
			if (mainClass == null) {
				System.out.println("Can not find class " + args[1] + "!");
				return;
			}
			Method method = mainClass.getMethod("main", String[].class);
			if (method == null) {
				System.out.println("Can not find main method from class " + args[1] + "!");
				return;
			}
			method.invoke(mainClass, (Object) new String[0]);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
	}
}
