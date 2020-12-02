package funlibs.binary;

public class Strings {
	public static String toHex(byte[] bytes) {
		if (bytes == null || bytes.length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder(bytes.length * 2);
		for (byte b : bytes) {
			sb.append(String.format("%02x", 0xff & b));
		}
		return sb.toString();
	}
}
