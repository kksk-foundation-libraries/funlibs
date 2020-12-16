package funlibs.concurrent;

import java.util.concurrent.Future;

public class ConcurrentUtil {
	private ConcurrentUtil() {
	}

	public static final void sleepSlient(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
		}
	}
	
	public static final <T> T getSilent(Future<T> future) {
		try {
			return future.get();
		} catch (Exception e) {
			return null;
		}
	}
}
