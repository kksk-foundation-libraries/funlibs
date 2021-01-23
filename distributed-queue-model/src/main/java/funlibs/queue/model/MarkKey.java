package funlibs.queue.model;


// Code generated by colf(1); DO NOT EDIT.
// The compiler used schema file model.colf.


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
public class MarkKey implements Serializable {

	/** The upper limit for serial byte sizes. */
	public static int colferSizeMax = 16 * 1024 * 1024;


	public long topicId;

	public int partition;

	/** Default constructor */
	public MarkKey() {
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
			if (buf == null || buf.length == 0)
				buf = new byte[Math.min(MarkKey.colferSizeMax, 2048)];
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
		public MarkKey next() throws IOException {
			if (in == null) return null;

			while (true) {
				if (this.i > this.offset) {
					try {
						MarkKey o = new MarkKey();
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
					if (offset == 0) this.buf = new byte[Math.min(MarkKey.colferSizeMax, this.buf.length * 4)];
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
	 * Gets the serial size estimate as an upper boundary, whereby
	 * {@link #marshal(byte[],int)} ≤ {@link #marshalFit()} ≤ {@link #colferSizeMax}.
	 * @return the number of bytes.
	 */
	public int marshalFit() {
		long n = 1L + 9 + 5;
		if (n < 0 || n > (long)MarkKey.colferSizeMax) return MarkKey.colferSizeMax;
		return (int) n;
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
		int n = 0;
		if (buf != null && buf.length != 0) try {
			n = marshal(buf, 0);
		} catch (BufferOverflowException e) {}
		if (n == 0) {
			buf = new byte[marshalFit()];
			n = marshal(buf, 0);
		}
		out.write(buf, 0, n);
		return buf;
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
			if (this.topicId != 0) {
				long x = this.topicId;
				if ((x & ~((1L << 49) - 1)) != 0) {
					buf[i++] = (byte) (0 | 0x80);
					buf[i++] = (byte) (x >>> 56);
					buf[i++] = (byte) (x >>> 48);
					buf[i++] = (byte) (x >>> 40);
					buf[i++] = (byte) (x >>> 32);
					buf[i++] = (byte) (x >>> 24);
					buf[i++] = (byte) (x >>> 16);
					buf[i++] = (byte) (x >>> 8);
					buf[i++] = (byte) (x);
				} else {
					buf[i++] = (byte) 0;
					while (x > 0x7fL) {
						buf[i++] = (byte) (x | 0x80);
						x >>>= 7;
					}
					buf[i++] = (byte) x;
				}
			}

			if (this.partition != 0) {
				int x = this.partition;
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

			buf[i++] = (byte) 0x7f;
			return i;
		} catch (ArrayIndexOutOfBoundsException e) {
			if (i - offset > MarkKey.colferSizeMax)
				throw new IllegalStateException(format("colfer: funlibs.queue/model.MarkKey exceeds %d bytes", MarkKey.colferSizeMax));
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
				long x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					if (shift == 56 || b >= 0) {
						x |= (b & 0xffL) << shift;
						break;
					}
					x |= (b & 0x7fL) << shift;
				}
				this.topicId = x;
				header = buf[i++];
			} else if (header == (byte) (0 | 0x80)) {
				this.topicId = (buf[i++] & 0xffL) << 56 | (buf[i++] & 0xffL) << 48 | (buf[i++] & 0xffL) << 40 | (buf[i++] & 0xffL) << 32
					| (buf[i++] & 0xffL) << 24 | (buf[i++] & 0xffL) << 16 | (buf[i++] & 0xffL) << 8 | (buf[i++] & 0xffL);
				header = buf[i++];
			}

			if (header == (byte) 1) {
				int x = 0;
				for (int shift = 0; true; shift += 7) {
					byte b = buf[i++];
					x |= (b & 0x7f) << shift;
					if (shift == 28 || b >= 0) break;
				}
				this.partition = x;
				header = buf[i++];
			} else if (header == (byte) (1 | 0x80)) {
				this.partition = (buf[i++] & 0xff) << 24 | (buf[i++] & 0xff) << 16 | (buf[i++] & 0xff) << 8 | (buf[i++] & 0xff);
				header = buf[i++];
			}

			if (header != (byte) 0x7f)
				throw new InputMismatchException(format("colfer: unknown header at byte %d", i - 1));
		} finally {
			if (i > end && end - offset < MarkKey.colferSizeMax) throw new BufferUnderflowException();
			if (i < 0 || i - offset > MarkKey.colferSizeMax)
				throw new SecurityException(format("colfer: funlibs.queue/model.MarkKey exceeds %d bytes", MarkKey.colferSizeMax));
			if (i > end) throw new BufferUnderflowException();
		}

		return i;
	}

	// {@link Serializable} version number.
	private static final long serialVersionUID = 2L;

	// {@link Serializable} Colfer extension.
	private void writeObject(ObjectOutputStream out) throws IOException {
		byte[] buf = new byte[marshalFit()];
		int n = marshal(buf, 0);
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
	 * Gets funlibs.queue/model.MarkKey.topicId.
	 * @return the value.
	 */
	public long getTopicId() {
		return this.topicId;
	}

	/**
	 * Sets funlibs.queue/model.MarkKey.topicId.
	 * @param value the replacement.
	 */
	public void setTopicId(long value) {
		this.topicId = value;
	}

	/**
	 * Sets funlibs.queue/model.MarkKey.topicId.
	 * @param value the replacement.
	 * @return {@code this}.
	 */
	public MarkKey withTopicId(long value) {
		this.topicId = value;
		return this;
	}

	/**
	 * Gets funlibs.queue/model.MarkKey.partition.
	 * @return the value.
	 */
	public int getPartition() {
		return this.partition;
	}

	/**
	 * Sets funlibs.queue/model.MarkKey.partition.
	 * @param value the replacement.
	 */
	public void setPartition(int value) {
		this.partition = value;
	}

	/**
	 * Sets funlibs.queue/model.MarkKey.partition.
	 * @param value the replacement.
	 * @return {@code this}.
	 */
	public MarkKey withPartition(int value) {
		this.partition = value;
		return this;
	}

	@Override
	public final int hashCode() {
		int h = 1;
		h = 31 * h + (int)(this.topicId ^ this.topicId >>> 32);
		h = 31 * h + this.partition;
		return h;
	}

	@Override
	public final boolean equals(Object o) {
		return o instanceof MarkKey && equals((MarkKey) o);
	}

	public final boolean equals(MarkKey o) {
		if (o == null) return false;
		if (o == this) return true;

		return this.topicId == o.topicId
			&& this.partition == o.partition;
	}

}
