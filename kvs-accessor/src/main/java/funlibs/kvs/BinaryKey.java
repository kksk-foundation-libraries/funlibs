package funlibs.kvs;

import java.io.Serializable;
import java.util.Arrays;

public class BinaryKey implements Serializable {
	private static final long serialVersionUID = -8924034347934180764L;

	public static BinaryKey of(byte[] data) {
		return new BinaryKey(data);
	}

	private byte[] data;

	public BinaryKey() {
	}

	public BinaryKey(byte[] data) {
		this.data = data;
	}

	public byte[] data() {
		return data;
	}

	public void data(byte[] data) {
		this.data = data;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(data);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BinaryKey other = (BinaryKey) obj;
		if (!Arrays.equals(data, other.data))
			return false;
		return true;
	}
}
