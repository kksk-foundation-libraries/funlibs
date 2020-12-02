package funlibs.serializer.kryo;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;

import funlibs.serializer.test.schema.TestObject;

public class KryoColferSerializerTest {

	@Test
	public void test() {
		Map<Integer, Class<?>> classes = new HashMap<>();
		classes.put(9999, TestObject.class);
		KryoPool kryoPool = new KryoPool.Builder(KryoColferSerializer.factory(classes)).build();
		Kryo kryo = kryoPool.borrow();
		TestObject testObject01 = new TestObject();
		testObject01.withField01(1).withField02(2).withField03(3).withField04(128);
		Output output = new Output(100);

		kryo.writeObject(output, testObject01);
		output.flush();
		byte[] bytes = output.toBytes();

		System.out.println(dump(bytes));
		assertTrue("serialized data is not collect.", "010001010202030380017f".equals(dump(bytes)));
		Input input = new Input(bytes);
		input.setPosition(0);
		TestObject testObject02 = kryo.readObjectOrNull(input, TestObject.class);
		assertTrue("field01 is unmatch", testObject01.getField01() == testObject02.getField01());
		assertTrue("field02 is unmatch", testObject01.getField02() == testObject02.getField02());
		assertTrue("field03 is unmatch", testObject01.getField03() == testObject02.getField03());
		assertTrue("field04 is unmatch", testObject01.getField04() == testObject02.getField04());
	}

	static String dump(byte[] data) {
		StringBuilder sb = new StringBuilder();
		for (byte b : data) {
			sb.append(String.format("%02x", 0xff & b));
		}
		return sb.toString();
	}
}
