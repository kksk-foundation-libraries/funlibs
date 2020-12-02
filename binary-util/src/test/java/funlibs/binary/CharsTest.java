package funlibs.binary;

import static org.junit.Assert.*;

import org.junit.Test;

public class CharsTest {

	@Test
	public void test() {
		char[] chars01 = "0123456789".toCharArray();
		byte[] bytes = Chars.fromCharArr(chars01);
		System.out.println(Strings.toHex(bytes));
		assertTrue("0030003100320033003400350036003700380039".equals(Strings.toHex(bytes)));
		char[] chars02 = Chars.toCharArr(bytes);
		System.out.println(new String(chars02));
		assertTrue("0123456789".equals(new String(chars02)));
	}

}
