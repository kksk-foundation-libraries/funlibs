package funlibs.serializer.kryo;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;

import funlibs.serializer.ColferDeserializer;
import funlibs.serializer.ColferSerializer;

public class KryoColferSerializer<T> extends Serializer<T> {
	private final ColferSerializer<T> serializer;
	private final ColferDeserializer<T> deserializer;

	public static void register(Kryo kryo, Class<?> clazz) {
		kryo.register(clazz, new KryoColferSerializer<>(clazz));
	}

	public static void register(Kryo kryo, Class<?> clazz, int id) {
		kryo.register(clazz, new KryoColferSerializer<>(clazz), id);
	}

	public static void register(Kryo kryo, Map<Integer, Class<?>> classes) {
		classes.forEach((id, clazz) -> {
			kryo.register(clazz, new KryoColferSerializer<>(clazz), id);
		});
	}

	@SafeVarargs
	public static KryoFactory factory(Map<Integer, Class<?>> classes, Consumer<Kryo>... extensions) {
		return new KryoFactory() {
			@Override
			public Kryo create() {
				Kryo kryo = new Kryo();
				classes.forEach((id, clazz) -> {
					kryo.register(clazz, new KryoColferSerializer<>(clazz), id);
				});
				for (Consumer<Kryo> consumer : extensions) {
					consumer.accept(kryo);
				}
				return kryo;
			}
		};
	}

	public KryoColferSerializer(Class<T> clazz) {
		serializer = ColferSerializer.of(clazz);
		deserializer = ColferDeserializer.of(clazz);
	}

	@Override
	public void write(Kryo kryo, Output output, T object) {
		output.writeBytes(serializer.serialize(object));
	}

	@Override
	public T read(Kryo kryo, Input input, Class<T> type) {
		int available = 0;
		try {
			available = input.available();
			return deserializer.deserialize(input.readBytes(available));
		} catch (IOException e) {
			return null;
		}
	}

}
