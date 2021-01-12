package funlibs.event.processing.persist.ignite;

import org.apache.ignite.Ignition;

public class IgniteTestContext {
	static final IgniteTestContext INSTANCE = new IgniteTestContext();

	private IgniteTestContext() {
		Ignition.start();
	}

	public static void load() {
	}
}
