package funlibs.serializer;

import static org.junit.Assert.*;

import org.junit.Test;

import funlibs.serializer.test.schema.TestObject;

public class ColferSerializerTest {

	@Test
	public void test() {
		TestObject testObject01 = new TestObject();
		testObject01.withField01(1).withField02(2).withField03(3).withField04(128);
		ColferSerializer<TestObject> serializer = ColferSerializer.of(TestObject.class);
		byte[] bytes = serializer.serialize(testObject01);
		System.out.println(dump(bytes));
		assertTrue("serialized data is not collect.", "0001010202030380017f".equals(dump(bytes)));
	}

	static String dump(byte[] data) {
		StringBuilder sb = new StringBuilder();
		for (byte b : data) {
			sb.append(String.format("%02x", 0xff & b));
		}
		return sb.toString();
	}
}
