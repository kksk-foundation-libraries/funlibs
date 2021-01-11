package funlibs.serializer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class ColferDeserializer<T> {
	private final Constructor<T> constructor;
	private final Method unmarshalMethod;

	public static <T> ColferDeserializer<T> of(Class<T> clazz) {
		return new ColferDeserializer<>(clazz);
	}

	public ColferDeserializer(Class<T> clazz) {
		Constructor<T> constructor = null;
		Method unmarshalMethod = null;
		try {
			constructor = clazz.getDeclaredConstructor();
			unmarshalMethod = clazz.getDeclaredMethod("unmarshal", byte[].class, Integer.TYPE);
		} catch (Exception e) {
		}
		this.constructor = constructor;
		this.unmarshalMethod = unmarshalMethod;
	}

	public T deserialize(byte[] bin) {
		if (bin == null || bin.length == 0) {
			return null;
		}
		try {
			T obj = constructor.newInstance();
			unmarshalMethod.invoke(obj, bin, 0);
			return obj;
		} catch (Exception e) {
		}
		return null;
	}
}
