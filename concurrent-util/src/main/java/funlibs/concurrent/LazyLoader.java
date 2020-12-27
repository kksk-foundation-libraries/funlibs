package funlibs.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class LazyLoader<T> implements Supplier<T> {
	private Supplier<T> supplier;
	private final AtomicBoolean loaded = new AtomicBoolean(false);
	private T reference;

	public LazyLoader() {
	}

	public LazyLoader(Supplier<T> supplier) {
		this.supplier = supplier;
	}

	public void supplier(Supplier<T> supplier) {
		this.supplier = supplier;
	}

	@Override
	public T get() {
		if (loaded.compareAndSet(false, true)) {
			reference = supplier.get();
		}
		return reference;
	}

}
