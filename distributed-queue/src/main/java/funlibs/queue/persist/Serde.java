package funlibs.queue.persist;

import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;

import funlibs.queue.model.AcknowledgeKey;
import funlibs.queue.model.EntryKey;
import funlibs.queue.model.EntryValue;
import funlibs.queue.model.KeyValue;
import funlibs.queue.model.MarkKey;
import funlibs.queue.model.PartitionKey;
import funlibs.queue.model.QueueInfo;
import funlibs.queue.model.QueueKey;
import funlibs.queue.model.QueueNodeInfo;
import funlibs.queue.model.QueueNodeKey;
import funlibs.queue.model.SubscribeKey;
import funlibs.queue.model.TransactionEntry;
import funlibs.queue.model.TransactionKey;
import funlibs.serializer.kryo.KryoColferSerializer;

public class Serde {
	private KryoPool pool;

	public Serde() {
		Map<Integer, Class<?>> map = new HashMap<>();
		int pos = 1;
		map.put(pos++, QueueKey.class);
		map.put(pos++, QueueInfo.class);
		map.put(pos++, QueueNodeKey.class);
		map.put(pos++, QueueNodeInfo.class);
		map.put(pos++, EntryKey.class);
		map.put(pos++, EntryValue.class);
		map.put(pos++, PartitionKey.class);
		map.put(pos++, KeyValue.class);
		map.put(pos++, TransactionKey.class);
		map.put(pos++, TransactionEntry.class);
		map.put(pos++, SubscribeKey.class);
		map.put(pos++, AcknowledgeKey.class);
		map.put(pos++, MarkKey.class);
		pool = new KryoPool.Builder(KryoColferSerializer.factory(map)).build();
	}

	public Kryo borrow() {
		return pool.borrow();
	}

	public void release(Kryo kryo) {
		pool.release(kryo);
	}

	public byte[] ser(Object object) {
		Output output = new Output(2048);
		Kryo kryo = borrow();
		try {
			kryo.writeClassAndObject(output, object);
			byte[] res = output.toBytes();
			return res;
		} finally {
			release(kryo);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T des(byte[] bytes, Class<T> clazz) {
		if (bytes == null)
			return null;
		Input input = new Input(bytes);
		Kryo kryo = borrow();
		try {
			T res = (T) kryo.readClassAndObject(input);
			return res;
		} finally {
			release(kryo);
		}
	}
}
