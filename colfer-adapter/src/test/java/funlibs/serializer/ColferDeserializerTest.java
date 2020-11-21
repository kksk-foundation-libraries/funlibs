package funlibs.serializer;

import static org.junit.Assert.*;

import org.junit.Test;

import funlibs.serializer.test.schema.TestObject;

public class ColferDeserializerTest {

	@Test
	public void test() {
		TestObject testObject01 = new TestObject();
		testObject01.withField01(1).withField02(2).withField03(3).withField04(128);
		ColferSerializer<TestObject> serializer = ColferSerializer.of(TestObject.class);
		byte[] bytes = serializer.serialize(testObject01);
		ColferDeserializer<TestObject> deserializer = ColferDeserializer.of(TestObject.class);
		TestObject testObject02 = deserializer.deserialize(bytes);
		assertTrue("field01 is unmatch", testObject01.getField01() == testObject02.getField01());
		assertTrue("field02 is unmatch", testObject01.getField02() == testObject02.getField02());
		assertTrue("field03 is unmatch", testObject01.getField03() == testObject02.getField03());
		assertTrue("field04 is unmatch", testObject01.getField04() == testObject02.getField04());
	}

}
