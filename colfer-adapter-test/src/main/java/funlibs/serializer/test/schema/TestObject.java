package funlibs.serializer.test.schema;


// Code generated by colf(1); DO NOT EDIT.


import static java.lang.String.format;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.InputMismatchException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;


/**
 * Data bean with built-in serialization support.

 * @author generated by colf(1)
 * @see <a href="https://github.com/pascaldekloe/colfer">Colfer's home</a>
 */
@javax.annotation.Generated(value="colf(1)", comments="Colfer from schema file schema.colf")
public class TestObject implements Serializable {

	/** The upper limit for serial byte sizes. */
	public static int colferSizeMax = 16 * 1024 * 1024;




	public int field01;

	public int field02;

	public int field03;

	public int field04;


	/** Default constructor */
	public TestObject() {
		init();
	}


	/** Colfer zero values. */
	private void init() {
	}

	/**
	 * {@link #reset(InputStream) Reusable} deserialization of Colfer streams.
	 */
	public static class Unmarshaller {

		/** The data source. */
		protected InputStream in;

		/** The read buffer. */
		public byte[] buf;

		/** The {@link #buf buffer}'s data start index, inclusive. */
		protected int offset;

		/** The {@link #buf buffer}'s data end index, exclusive. */
		protected int i;


		/**
		 * @param in the data source or {@code null}.
		 * @param buf the initial buffer or {@code null}.
		 */
		public Unmarshaller(InputStream in, byte[] buf) {
			// TODO: better size estimation
			if (buf == null || buf.length == 0)
				buf = new byte[Math.min(TestObject.colferSizeMax, 2048)];
			this.buf = buf;
			reset(in);
		}

		/**
		 * Reuses the marshaller.
		 * @param in the data source or {@code null}.
		 * @throws IllegalStateException on pending data.
		 */
		public void reset(InputStream in) {
			if (this.i != this.offset) throw new IllegalStateException("colfer: pending data");
			this.in = in;
			this.offset = 0;
			this.i = 0;
		}

		/**
		 * Deserializes the following object.
		 * @return the result or {@code null} when EOF.
		 * @throws IOException from the input stream.
		 * @throws SecurityException on an upper limit breach defined by {@link #colferSizeMax}.
		 * @throws InputMismatchException when the data does not match this object's schema.
		 */
		public TestObject next() throws IOException {
			if (in == null) return null;

			while (true) {
				if (this.i > this.offset) {
					try {
						TestObject o = new TestObject();
						this.offset = o.unmarshal(this.buf, this.offset, this.i);
						return o;
					} catch (BufferUnderflowException e) {
					}
				}
				// not enough data

				if (this.i <= this.offset) {
					this.offset = 0;
					this.i = 0;
				} else if (i == buf.length) {
					byte[] src = this.buf;
					// TODO: better size estimation
					if (offset == 0) this.buf = new byte[Math.min(TestObject.colferSizeMax, this.buf.length * 4)];
					System.arraycopy(src, this.offset, this.buf, 0, this.i - this.offset);
					this.i -= this.offset;
					this.offset = 0;
				}
				assert this.i < this.buf.length;

				int n = in.read(buf, i, buf.length - i);
				if (n < 0) {
					if (this.i > this.offset)
						throw new InputMismatchException("colfer: pending data with EOF");
					return null;
				}
				assert n > 0;
				i += n;
			}
		}

	}


	/**
	 * Serializes the object.
	 * @param out the data destination.
	 * @param buf the initial buffer or {@code null}.
	 * @return the final buffer. When the serial fits into {@code buf} then the return is {@code buf}.
	 *  Otherwise the return is a new buffer, large enough to hold the whole serial.
	 * @throws IOException from {@code out}.
	 * @throws IllegalStateException on an upper limit breach defined by {@link #colferSizeMax}.
	 */
	public byte[] marshal(OutputStream out, byte[] buf) throws IOException {
		// TODO: better size estimation
		if (buf == null || buf.length == 0)
			buf = new byte[Math.min(TestObject.colferSizeMax, 2048)];

		while (true) {
			int i;
			try {
				i = marshal(buf, 0);
			} catch (BufferOverflowException e) {
				buf = new byte[Math.min(TestObject.colferSizeMax, buf.length * 4)];
				continue;
			}

			out.write(buf, 0, i);
			return buf;
		}
	}

	/**
	 * Serializes the object.
	 * @param buf the data destination.
	 * @param offset the initial index for {@code buf}, inclusive.
	 * @return the final index for {@code buf}, exclusive.
	 * @throws BufferOverflowException when {@code buf} is too small.
	 * @throws IllegalStateException on an upper limit breach defined by {@link #colferSizeMax}.
	 */
	public int marshal(byte[] buf, int offset) {
		int i = offset;

		try {
			if (this.field01 != 0) {
				int x = this.field01;
				if ((x & ~((1 << 21) - 1)) != 0) {
					buf[i++] = (byte) (0 | 0x80);
					buf[i++] = (byte) (x >>> 24);
					buf[i++] = (byte) (x >>> 16);
					buf[i++] = (byte) (x >>> 8);
				} else {
					buf[i++] = (byte) 0;
					while (x > 0x7f) {
						buf[i++] = (byte) (x | 0x80);
						x >>>= 7;
					}
				}
				buf[i++] = (byte) x;
			}

			if (this.field02 != 0) {
				int x = this.field02;
				if ((x & ~((1 << 21) - 1)) != 0) {
					buf[i++] = (byte) (1 | 0x80);
					buf[i++] = (byte) (x >>> 24);
					buf[i++] = (byte) (x >>> 16);
					buf[i++] = (byte) (x >>> 8);
				} else {
					buf[i++] = (byte) 1;
					while (x > 0x7f) {
						buf[i++] = (byte) (x | 0x80);
						x >>>= 7;
					}
				}
				buf[i++] = (byte) x;
			}

			if (this.field03 != 0) {
				int x = this.field03;
				if ((x & ~((1 << 21) - 1)) != 0) {
					buf[i++] = (byte) (2 | 0x80);
					buf[i++] = (byte) (x >>> 24);
					buf[i++] = (byte) (x >>> 16);
					buf[i++] = (byte) (x >>> 8);
				} else {
					buf[i++] = (byte) 2;
					while (x > 0x7f) {
						buf[i++] = (byte) (x | 0x80);
						x >>>= 7;
					}
				}
				buf[i++] = (byte) x;
			}

			if (this.field04 != 0) {
				int x = this.field04;
				if ((x & ~((1 << 21) - 1)) != 0) {
					buf[i++] = (byte) (3 | 0x80);
					buf[i++] = (byte) (x >>> 24);
					buf[i++] = (byte) (x >>> 16);
					buf[i++] = (byte) (x >>> 8);
				} else {
					buf[i++] = (byte) 3;
					while (x > 0x7f) {
						buf[i++] = (byte) (x | 0x80);
						x >>>= 7;
					}
				}
				buf[i++] = (byte) x;
			}

			buf[i++] = (byte) 0x7f;
			return i;
		} catch (ArrayIndexOutOfBoundsException e) {
			if (i - offset > TestObject.colferSizeMax)
				throw new IllegalStateException(format("colfer: funlibs/serializer/test/schema.TestObject exceeds %d bytes", TestObject.colferSizeMax));
			if (i > buf.length) throw new BufferOverflowException();
			throw e;
		}
	}

	/**
	 * Deserializes the object.
	 * @param buf the data source.
	 * @param offset the initial index for {@code buf}, inclusive.
	 * @return the final index for {@code buf}, exclusive.
	 * @throws BufferUnderflowException when {@code buf} is incomplete. (EOF)
	 * @throws SecurityException on an upper limit breach defined by {@link #colferSizeMax}.
	 * @throws InputMismatchException when the data does not match this object's schema.
	 */
	public int unmarshal(byte[] buf, int offset) {
		return unmarshal(buf, offset, buf.length);
	}

	/**
	 * Deserializes the object.
	 * @param buf the data source.
	 * @param offset the initial index for {@code buf}, inclusive.
	 * @param end the index limit for {@code buf}, exclusive.
	 * @return the final index for {@code buf}, exclusive.
	 * @throws BufferUnderflowException when {@code buf} is incomplete. (EOF)
	 * @throws SecurityException on an upper limit breach defined by {@link #colferSizeMax}.
	 * @throws InputMismatchException when the data does not match this object's schema.
	 */
	public int unmarshal(byte[] buf, int offset, int end) {
		if (end > buf.length) end = buf.length;
		int i = offset;

		try {
			byte header = buf[i++];

			if (header == (byte) 0) {
				int x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					x |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				this.field01 = x;
				header = buf[i++];
			} else if (header == (byte) (0 | 0x80)) {
				this.field01 = (buf[i++] & 0xff) << 24 | (buf[i++] & 0xff) << 16 | (buf[i++] & 0xff) << 8 | (buf[i++] & 0xff);
				header = buf[i++];
			}

			if (header == (byte) 1) {
				int x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					x |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				this.field02 = x;
				header = buf[i++];
			} else if (header == (byte) (1 | 0x80)) {
				this.field02 = (buf[i++] & 0xff) << 24 | (buf[i++] & 0xff) << 16 | (buf[i++] & 0xff) << 8 | (buf[i++] & 0xff);
				header = buf[i++];
			}

			if (header == (byte) 2) {
				int x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					x |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				this.field03 = x;
				header = buf[i++];
			} else if (header == (byte) (2 | 0x80)) {
				this.field03 = (buf[i++] & 0xff) << 24 | (buf[i++] & 0xff) << 16 | (buf[i++] & 0xff) << 8 | (buf[i++] & 0xff);
				header = buf[i++];
			}

			if (header == (byte) 3) {
				int x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					x |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				this.field04 = x;
				header = buf[i++];
			} else if (header == (byte) (3 | 0x80)) {
				this.field04 = (buf[i++] & 0xff) << 24 | (buf[i++] & 0xff) << 16 | (buf[i++] & 0xff) << 8 | (buf[i++] & 0xff);
				header = buf[i++];
			}

			if (header != (byte) 0x7f)
				throw new InputMismatchException(format("colfer: unknown header at byte %d", i - 1));
		} finally {
			if (i > end && end - offset < TestObject.colferSizeMax) throw new BufferUnderflowException();
			if (i < 0 || i - offset > TestObject.colferSizeMax)
				throw new SecurityException(format("colfer: funlibs/serializer/test/schema.TestObject exceeds %d bytes", TestObject.colferSizeMax));
			if (i > end) throw new BufferUnderflowException();
		}

		return i;
	}

	// {@link Serializable} version number.
	private static final long serialVersionUID = 4L;

	// {@link Serializable} Colfer extension.
	private void writeObject(ObjectOutputStream out) throws IOException {
		// TODO: better size estimation
		byte[] buf = new byte[1024];
		int n;
		while (true) try {
			n = marshal(buf, 0);
			break;
		} catch (BufferUnderflowException e) {
			buf = new byte[4 * buf.length];
		}

		out.writeInt(n);
		out.write(buf, 0, n);
	}

	// {@link Serializable} Colfer extension.
	private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
		init();

		int n = in.readInt();
		byte[] buf = new byte[n];
		in.readFully(buf);
		unmarshal(buf, 0);
	}

	// {@link Serializable} Colfer extension.
	private void readObjectNoData() throws ObjectStreamException {
		init();
	}

	/**
	 * Gets funlibs/serializer/test/schema.TestObject.field01.
	 * @return the value.
	 */
	public int getField01() {
		return this.field01;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field01.
	 * @param value the replacement.
	 */
	public void setField01(int value) {
		this.field01 = value;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field01.
	 * @param value the replacement.
	 * @return {link this}.
	 */
	public TestObject withField01(int value) {
		this.field01 = value;
		return this;
	}

	/**
	 * Gets funlibs/serializer/test/schema.TestObject.field02.
	 * @return the value.
	 */
	public int getField02() {
		return this.field02;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field02.
	 * @param value the replacement.
	 */
	public void setField02(int value) {
		this.field02 = value;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field02.
	 * @param value the replacement.
	 * @return {link this}.
	 */
	public TestObject withField02(int value) {
		this.field02 = value;
		return this;
	}

	/**
	 * Gets funlibs/serializer/test/schema.TestObject.field03.
	 * @return the value.
	 */
	public int getField03() {
		return this.field03;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field03.
	 * @param value the replacement.
	 */
	public void setField03(int value) {
		this.field03 = value;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field03.
	 * @param value the replacement.
	 * @return {link this}.
	 */
	public TestObject withField03(int value) {
		this.field03 = value;
		return this;
	}

	/**
	 * Gets funlibs/serializer/test/schema.TestObject.field04.
	 * @return the value.
	 */
	public int getField04() {
		return this.field04;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field04.
	 * @param value the replacement.
	 */
	public void setField04(int value) {
		this.field04 = value;
	}

	/**
	 * Sets funlibs/serializer/test/schema.TestObject.field04.
	 * @param value the replacement.
	 * @return {link this}.
	 */
	public TestObject withField04(int value) {
		this.field04 = value;
		return this;
	}

	@Override
	public final int hashCode() {
		int h = 1;
		h = 31 * h + this.field01;
		h = 31 * h + this.field02;
		h = 31 * h + this.field03;
		h = 31 * h + this.field04;
		return h;
	}

	@Override
	public final boolean equals(Object o) {
		return o instanceof TestObject && equals((TestObject) o);
	}

	public final boolean equals(TestObject o) {
		if (o == null) return false;
		if (o == this) return true;
		return o.getClass() == TestObject.class
			&& this.field01 == o.field01
			&& this.field02 == o.field02
			&& this.field03 == o.field03
			&& this.field04 == o.field04;
	}

}
