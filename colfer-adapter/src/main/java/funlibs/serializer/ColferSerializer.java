package funlibs.serializer;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

public class ColferSerializer<T> {
	private final Method marshalMethod;

	public static <T> ColferSerializer<T> of(Class<T> clazz) {
		return new ColferSerializer<>(clazz);
	}

	public ColferSerializer(Class<T> clazz) {
		Method marshalMethod = null;
		try {
			marshalMethod = clazz.getDeclaredMethod("marshal", OutputStream.class, byte[].class);
		} catch (Exception e) {
		}
		this.marshalMethod = marshalMethod;
	}

	public byte[] serialize(T obj) {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			marshalMethod.invoke(obj, out, null);
			return out.toByteArray();
		} catch (Exception e) {
		}
		return null;
	}
}
