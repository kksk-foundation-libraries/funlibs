package funlibs.event.processing.persist.ignite;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.logging.SimpleLogging;

public class IgniteBinaryStoreTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		IgniteTestContext.load();
	}

	static final SimpleLogging ll = SimpleLogging.forPackageDebug();
	private static final Logger LOG = LoggerFactory.getLogger(IgniteBinaryStoreTest.class);

	@Test
	public void test_001_put_get() {
		Ignite ignite = Ignition.ignite();
		IgniteBinaryStore store = new IgniteBinaryStore(ignite, "test");
		store.clear();
		byte[] testKey_01 = "testKey".getBytes();
		LOG.debug("{} testKey_01:{}", "test_001", Strings.toHex(testKey_01));
		byte[] testValue_01 = "testValue".getBytes();
		LOG.debug("{} testValue_01:{}", "test_001", Strings.toHex(testValue_01));
		store.put(testKey_01, testValue_01);
		byte[] testKey_02 = "testKey".getBytes();
		LOG.debug("{} testKey_02:{}", "test_001", Strings.toHex(testKey_02));
		byte[] testValue_02 = "testValue".getBytes();
		LOG.debug("{} testValue_02:{}", "test_001", Strings.toHex(testValue_02));
		byte[] resultValue = store.get(testKey_02);
		LOG.debug("{} resultValue:{}", "test_001", Strings.toHex(resultValue));
		assertTrue("unmatch result expected:[" + Strings.toHex(testValue_02) + "], output:[" + Strings.toHex(resultValue) + "]", Arrays.equals(testValue_02, resultValue));
	}

	@Test
	public void test_002_put_putIfAbsent() {
		Ignite ignite = Ignition.ignite();
		IgniteBinaryStore store = new IgniteBinaryStore(ignite, "test");
		store.clear();
		byte[] testKey_01 = "testKey".getBytes();
		LOG.debug("{} testKey_01:{}", "test_002", Strings.toHex(testKey_01));
		byte[] testValue_01 = "testValue".getBytes();
		LOG.debug("{} testValue_01:{}", "test_002", Strings.toHex(testValue_01));
		store.put(testKey_01, testValue_01);
		byte[] testKey_02 = "testKey".getBytes();
		LOG.debug("{} testKey_02:{}", "test_002", Strings.toHex(testKey_02));
		byte[] testValue_02 = "testValue2".getBytes();
		LOG.debug("{} testValue_02:{}", "test_002", Strings.toHex(testValue_02));
		boolean resultValue = store.putIfAbsent(testKey_02, testValue_02);
		LOG.debug("{} resultValue:{}", "test_002", resultValue);
		assertTrue("unmatch result expected:[false], output:[" + resultValue + "]", !resultValue);
	}

	@Test
	public void test_003_put_remove_get() {
		Ignite ignite = Ignition.ignite();
		IgniteBinaryStore store = new IgniteBinaryStore(ignite, "test");
		store.clear();
		byte[] testKey_01 = "testKey".getBytes();
		LOG.debug("{} testKey_01:{}", "test_003", Strings.toHex(testKey_01));
		byte[] testValue_01 = "testValue".getBytes();
		LOG.debug("{} testValue_01:{}", "test_003", Strings.toHex(testValue_01));
		store.put(testKey_01, testValue_01);
		byte[] testKey_02 = "testKey".getBytes();
		LOG.debug("{} testKey_02:{}", "test_003", Strings.toHex(testKey_02));
		boolean resultValue = store.remove(testKey_02);
		LOG.debug("{} resultValue:{}", "test_003", resultValue);
		byte[] resultValue2 = store.get(testKey_02);
		LOG.debug("{} resultValue2:{}", "test_003", Strings.toHex(resultValue2));
		assertTrue("unmatch result expected:[true], output:[" + resultValue + "]", resultValue);
		assertTrue("unmatch result expected:[null], output:[" + Strings.toHex(resultValue2) + "]", null == resultValue2);
	}

	@Test
	public void test_004_put_getAndRemove() {
		Ignite ignite = Ignition.ignite();
		IgniteBinaryStore store = new IgniteBinaryStore(ignite, "test");
		store.clear();
		byte[] testKey_01 = "testKey".getBytes();
		LOG.debug("{} testKey_01:{}", "test_004", Strings.toHex(testKey_01));
		byte[] testValue_01 = "testValue".getBytes();
		LOG.debug("{} testValue_01:{}", "test_004", Strings.toHex(testValue_01));
		store.put(testKey_01, testValue_01);
		byte[] testKey_02 = "testKey".getBytes();
		LOG.debug("{} testKey_02:{}", "test_004", Strings.toHex(testKey_02));
		byte[] testValue_02 = "testValue".getBytes();
		LOG.debug("{} testValue_02:{}", "test_004", Strings.toHex(testValue_02));
		byte[] resultValue = store.getAndRemove(testKey_02);
		LOG.debug("{} resultValue:{}", "test_004", Strings.toHex(resultValue));
		byte[] resultValue2 = store.get(testKey_02);
		LOG.debug("{} resultValue2:{}", "test_004", Strings.toHex(resultValue2));
		assertTrue("unmatch result expected:[" + Strings.toHex(testValue_02) + "], output:[" + resultValue + "]", Arrays.equals(testValue_02, resultValue));
		assertTrue("unmatch result expected:[null], output:[" + Strings.toHex(resultValue2) + "]", null == resultValue2);
	}
}
