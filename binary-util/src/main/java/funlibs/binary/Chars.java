package funlibs.binary;

public class Chars {
	private static final char[] BLANK_CHAR_ARR = {};
	private static final byte[] BLANK_BYTE_ARR = {};

	public static char[] toCharArr(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return BLANK_CHAR_ARR;
		}
		char[] chars = new char[bytes.length / Character.BYTES];
		for (int i = 0; i < chars.length; i++) {
			chars[i] = Bits.getChar(bytes, i * Character.BYTES);
		}
		return chars;
	}

	public static byte[] fromCharArr(char[] chars) {
		if (chars == null || chars.length == 0) {
			return BLANK_BYTE_ARR;
		}
		byte[] bytes = new byte[chars.length * Character.BYTES];
		for (int i = 0; i < chars.length; i++) {
			Bits.putChar(bytes, i * Character.BYTES, chars[i]);
		}
		return bytes;
	}
}
