package funlibs.binary;

import static org.junit.Assert.*;

import org.junit.Test;

public class StringsTest {

	@Test
	public void test() {
		long l = 0;
		for (int i = 0; i < Long.BYTES; i++) {
			l <<= Byte.SIZE;
			l |= (i + 1);
		}
		byte[] bytes = new byte[Long.BYTES];
		Bits.putLong(bytes, 0, l);
		System.out.println(Strings.toHex(bytes));
		assertTrue("wrong format.", "0102030405060708".equals(Strings.toHex(bytes)));
	}

}
