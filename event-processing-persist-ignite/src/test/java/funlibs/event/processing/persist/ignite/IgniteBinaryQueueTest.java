package funlibs.event.processing.persist.ignite;

import static org.junit.Assert.assertTrue;

import org.apache.ignite.Ignition;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.event.processing.persist.BinaryQueue;
import funlibs.logging.SimpleLogging;

public class IgniteBinaryQueueTest {
	static final SimpleLogging ll = SimpleLogging.forPackageDebug();
	private static final Logger LOG = LoggerFactory.getLogger(IgniteBinaryQueueTest.class);

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		IgniteTestContext.load();
	}

	@Test
	public void test_001() {
		int pattern = 1;
		String[] input = { "value1" };
		String[] action = { "addLast", };
		String[] expect_getFirst = { "value1" };
		String[] expect_getLast = { "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_002() {
		int pattern = 2;
		String[] input = { "value1" };
		String[] action = { "addFirst" };
		String[] expect_getFirst = { "value1" };
		String[] expect_getLast = { "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_003() {
		int pattern = 3;
		String[] input = { "value1", "value2" };
		String[] action = { "addLast", "addLast", };
		String[] expect_getFirst = { "value1", "value1" };
		String[] expect_getLast = { "value1", "value2" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_004() {
		int pattern = 4;
		String[] input = { "value1", "value2" };
		String[] action = { "addLast", "addFirst", };
		String[] expect_getFirst = { "value1", "value2" };
		String[] expect_getLast = { "value1", "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_005() {
		int pattern = 5;
		String[] input = { "value1", "value2" };
		String[] action = { "addFirst", "addLast", };
		String[] expect_getFirst = { "value1", "value1" };
		String[] expect_getLast = { "value1", "value2" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_006() {
		int pattern = 6;
		String[] input = { "value1", "value2" };
		String[] action = { "addFirst", "addFirst", };
		String[] expect_getFirst = { "value1", "value2" };
		String[] expect_getLast = { "value1", "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_007() {
		int pattern = 7;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addLast", "addLast", "addLast", };
		String[] expect_getFirst = { "value1", "value1", "value1" };
		String[] expect_getLast = { "value1", "value2", "value3" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_008() {
		int pattern = 8;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addLast", "addFirst", "addLast" };
		String[] expect_getFirst = { "value1", "value2", "value2" };
		String[] expect_getLast = { "value1", "value1", "value3" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_009() {
		int pattern = 9;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addFirst", "addLast", "addLast" };
		String[] expect_getFirst = { "value1", "value1", "value1" };
		String[] expect_getLast = { "value1", "value2", "value3" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_010() {
		int pattern = 10;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addFirst", "addFirst", "addLast" };
		String[] expect_getFirst = { "value1", "value2", "value2" };
		String[] expect_getLast = { "value1", "value1", "value3" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_011() {
		int pattern = 11;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addLast", "addLast", "addFirst", };
		String[] expect_getFirst = { "value1", "value1", "value3" };
		String[] expect_getLast = { "value1", "value2", "value2" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_012() {
		int pattern = 12;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addLast", "addFirst", "addFirst" };
		String[] expect_getFirst = { "value1", "value2", "value3" };
		String[] expect_getLast = { "value1", "value1", "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_013() {
		int pattern = 13;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addFirst", "addLast", "addFirst" };
		String[] expect_getFirst = { "value1", "value1", "value3" };
		String[] expect_getLast = { "value1", "value2", "value2" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_014() {
		int pattern = 14;
		String[] input = { "value1", "value2", "value3" };
		String[] action = { "addFirst", "addFirst", "addFirst" };
		String[] expect_getFirst = { "value1", "value2", "value3" };
		String[] expect_getLast = { "value1", "value1", "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_015() {
		int pattern = 15;
		String[] input = { "value1", "value2", "-" };
		String[] action = { "addLast", "addLast", "removeLast" };
		String[] expect_getFirst = { "value1", "value1", "value1" };
		String[] expect_getLast = { "value1", "value2", "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_016() {
		int pattern = 16;
		String[] input = { "value1", "value2", "-" };
		String[] action = { "addLast", "addFirst", "removeLast" };
		String[] expect_getFirst = { "value1", "value2", "value2" };
		String[] expect_getLast = { "value1", "value1", "value2" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_017() {
		int pattern = 17;
		String[] input = { "value1", "value2", "-" };
		String[] action = { "addLast", "addLast", "removeFirst" };
		String[] expect_getFirst = { "value1", "value1", "value2" };
		String[] expect_getLast = { "value1", "value2", "value2" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}

	@Test
	public void test_018() {
		int pattern = 18;
		String[] input = { "value1", "value2", "-" };
		String[] action = { "addLast", "addFirst", "removeFirst" };
		String[] expect_getFirst = { "value1", "value2", "value1" };
		String[] expect_getLast = { "value1", "value1", "value1" };

		doTest(pattern, input, action, expect_getFirst, expect_getLast);
	}


	private void doTest(int pattern, String[] input, String[] action, String[] expect_getFirst, String[] expect_getLast) {
		LOG.debug("#" + pattern + ". " + String.join("->", action));

		IgniteBinaryStore store = new IgniteBinaryStore(Ignition.ignite(), "store");
		store.clear();
		BinaryQueue binaryQueue = new BinaryQueue("key".getBytes(), store);
		byte[] getFirst;
		byte[] getLast;
		for (int i = 0; i < input.length; i++) {
			int step = i + 1;
			act(binaryQueue, action[i], input[i]);
			getFirst = binaryQueue.getFirst();
			getLast = binaryQueue.getLast();
			LOG.debug("#" + pattern + "." + step + " " + action[i] + " -> getFirst:[{}], getLast:[{}]", Strings.toHex(getFirst), Strings.toHex(getLast));
			assertTrue("step:" + step + " getFirst is not match", expect_getFirst[i].equals(new String(getFirst)));
			assertTrue("step:" + step + " getLast is not match", expect_getLast[i].equals(new String(getLast)));
		}
//		store.dump();
	}

	private void act(BinaryQueue binaryQueue, String action, String input) {
		switch (action) {
		case "addLast":
			binaryQueue.addLast(input.getBytes());
			break;
		case "addFirst":
			binaryQueue.addFirst(input.getBytes());
			break;
		case "removeLast":
			binaryQueue.removeLast();
			break;
		case "removeFirst":
			binaryQueue.removeFirst();
			break;
		}
	}

}
