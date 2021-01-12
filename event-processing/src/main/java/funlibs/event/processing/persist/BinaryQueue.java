package funlibs.event.processing.persist;

import java.util.AbstractSequentialList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import funlibs.event.processing.model.QueueKey;
import funlibs.event.processing.model.QueueNode;
import funlibs.event.processing.model.QueueWaterMarkKey;
import funlibs.event.processing.model.QueueWaterMarkValue;
import funlibs.serializer.ColferDeserializer;
import funlibs.serializer.ColferSerializer;

public class BinaryQueue extends AbstractSequentialList<byte[]> {
	private static final byte TYPE_SIZE = 0;
	private static final byte TYPE_NODE = 1;
	private static final byte TYPE_FIRST = 2;
	private static final byte TYPE_LAST = 3;
	private static final byte TYPE_SEQ = 4;
	private static final byte TYPE_VALUE = 5;
	private static final Long NONE = Long.valueOf(0L);

	private final AtomicLong seq = new AtomicLong();
	private static final ColferSerializer<QueueKey> QUEUE_KEY_SERIALIZER = ColferSerializer.of(QueueKey.class);
	private static final ColferSerializer<QueueNode> QUEUE_NODE_SERIALIZER = ColferSerializer.of(QueueNode.class);
	private static final ColferDeserializer<QueueNode> QUEUE_NODE_DESERIALIZER = ColferDeserializer.of(QueueNode.class);
	private static final ColferSerializer<QueueWaterMarkKey> QUEUE_WATER_MARK_KEY_SERIALIZER = ColferSerializer.of(QueueWaterMarkKey.class);
	private static final ColferSerializer<QueueWaterMarkValue> QUEUE_WATER_MARK_VALUE_SERIALIZER = ColferSerializer.of(QueueWaterMarkValue.class);
	private static final ColferDeserializer<QueueWaterMarkValue> QUEUE_WATER_MARK_VALUE_DESERIALIZER = ColferDeserializer.of(QueueWaterMarkValue.class);

	private int size = 0;
	private Node first;
	private Node last;

	private final byte[] key;
	private final byte[] key_first;
	private final byte[] key_last;
	private final byte[] key_seq;
	private final byte[] key_size;
	private final BinaryStore store;

	public BinaryQueue(byte[] key, BinaryStore store) {
		this.key = key;
		this.store = store;
		this.key_first = QUEUE_WATER_MARK_KEY_SERIALIZER.serialize(new QueueWaterMarkKey().withDataType(TYPE_FIRST).withKey(key));
		this.key_last = QUEUE_WATER_MARK_KEY_SERIALIZER.serialize(new QueueWaterMarkKey().withDataType(TYPE_LAST).withKey(key));
		this.key_seq = QUEUE_WATER_MARK_KEY_SERIALIZER.serialize(new QueueWaterMarkKey().withDataType(TYPE_SEQ).withKey(key));
		this.key_size = QUEUE_WATER_MARK_KEY_SERIALIZER.serialize(new QueueWaterMarkKey().withDataType(TYPE_SIZE).withKey(key));
		first = node(store.get(key_first));
		last = node(store.get(key_last));
		seq.set(lastSeq());
		loadSize();
	}

	private Node node(byte[] value) {
		if (value == null)
			return null;
		QueueWaterMarkValue queueWaterMarkValue = QUEUE_WATER_MARK_VALUE_DESERIALIZER.deserialize(value);
		Long curr = queueWaterMarkValue.getCurr();
		return loadNode(curr);
	}

	public BinaryQueue(byte[] key, BinaryStore store, Collection<? extends byte[]> c) {
		this(key, store);
		addAll(c);
	}

	private byte[] nodeKey(byte typeId, Long id) {
		return QUEUE_KEY_SERIALIZER.serialize(new QueueKey().withDataType(typeId).withKey(key).withCurr(id));
	}

	private byte[] nodeValue(Long prev, Long next) {
		return QUEUE_NODE_SERIALIZER.serialize(new QueueNode().withPrev(prev).withNext(next));
	}

	private void putData(Long id, byte[] value) {
		store.put(nodeKey(TYPE_VALUE, id), value);
	}

	private byte[] getValue(Long id) {
		return store.get(nodeKey(TYPE_VALUE, id));
	}

	private void deleteValue(Long id) {
		store.remove(nodeKey(TYPE_VALUE, id));
	}

	private void saveNode(Node node) {
		store.put(nodeKey(TYPE_NODE, node.curr), nodeValue(node.prev, node.next));
	}

	private void deleteNode(Long curr) {
		store.remove(nodeKey(TYPE_NODE, curr));
	}

	private Node loadNode(Long id) {
		byte[] value = store.get(nodeKey(TYPE_NODE, id));
		if (value == null) {
			return null;
		}
		QueueNode node = QUEUE_NODE_DESERIALIZER.deserialize(value);
		Long curr = id;
		Long prev = node.getPrev();
		Long next = node.getNext();
		return new Node(prev, curr, next);
	}

	private void loadSize() {
		QueueWaterMarkValue value = QUEUE_WATER_MARK_VALUE_DESERIALIZER.deserialize(store.get(key_size));
		if (value == null) {
			size = 0;
		} else {
			size = (int) value.getCurr();
		}
	}

	private void saveSize() {
		if (size == 0) {
			store.remove(key_size);
		} else {
			byte[] value = QUEUE_WATER_MARK_VALUE_SERIALIZER.serialize(new QueueWaterMarkValue().withCurr(size));
			store.put(key_size, value);
		}
	}

	private void saveFirst() {
		if (first == null) {
			store.remove(key_first);
			store.remove(key_seq);
			seq.set(0L);
		} else {
			byte[] value = QUEUE_WATER_MARK_VALUE_SERIALIZER.serialize(new QueueWaterMarkValue().withCurr(first.curr));
			store.put(key_first, value);
		}
	}

	private void saveLast() {
		if (last == null) {
			store.remove(key_last);
			store.remove(key_seq);
			seq.set(0L);
		} else {
			byte[] value = QUEUE_WATER_MARK_VALUE_SERIALIZER.serialize(new QueueWaterMarkValue().withCurr(last.curr));
			store.put(key_last, value);
		}
	}

	private Long lastSeq() {
		byte[] value = store.get(key_seq);
		if (value == null) {
			return 0L;
		}
		return QUEUE_WATER_MARK_VALUE_DESERIALIZER.deserialize(value).getCurr();
	}

	private Long id() {
		if (seq.get() % 100L == 0L) {
			byte[] value = QUEUE_WATER_MARK_VALUE_SERIALIZER.serialize(new QueueWaterMarkValue().withCurr(seq.get() + 100L));
			store.put(key_seq, value);
		}
		long id = seq.incrementAndGet();
		return id;
	}

	/**
	 * Links e as first element.
	 */
	private void linkFirst(Long id) {
		final Node f = first;
		final Node newNode = new Node(NONE, id, f == null ? NONE : f.curr);
		saveNode(newNode);
		first = newNode;
		saveFirst();
		if (f == null) {
			last = newNode;
			saveLast();
		} else {
			f.prev = newNode.curr;
			saveNode(f);
		}
		size++;
		saveSize();
		modCount++;
	}

	/**
	 * Links e as last element.
	 */
	void linkLast(Long id) {
		final Node l = last;
		final Node newNode = new Node(l == null ? NONE : l.curr, id, NONE);
		saveNode(newNode);
		last = newNode;
		saveLast();
		if (l == null) {
			first = newNode;
			saveFirst();
		} else {
			l.next = newNode.curr;
			saveNode(l);
		}
		size++;
		saveSize();
		modCount++;
	}

	/**
	 * Inserts element e before non-null Node succ.
	 */
	void linkBefore(Long id, Node succ) {
		// assert succ != null;
		final Node pred = NONE.equals(succ.prev) ? null : loadNode(succ.prev);
		final Node newNode = new Node(pred == null ? NONE : pred.curr, id, succ.curr);
		saveNode(newNode);
		succ.prev = newNode.curr;
		saveNode(succ);
		if (pred == null) {
			first = newNode;
			saveFirst();
		} else {
			pred.next = newNode.curr;
			saveNode(pred);
		}
		size++;
		saveSize();
		modCount++;
	}

	/**
	 * Unlinks non-null first node f.
	 */
	private Long unlinkFirst(Node f) {
		// assert f == first && f != null;
		final Long curr = f.curr;
		final Node next = NONE.equals(f.next) ? null : loadNode(f.next);
		f.curr = NONE;
		f.next = NONE; // help GC
		first = next;
		saveFirst();
		if (next == null) {
			last = null;
			saveLast();
		} else {
			next.prev = NONE;
			saveNode(next);
		}
		deleteNode(curr);
		size--;
		saveSize();
		modCount++;
		return curr;
	}

	/**
	 * Unlinks non-null last node l.
	 */
	private Long unlinkLast(Node l) {
		// assert l == last && l != null;
		final Long curr = l.curr;
		final Node prev = NONE.equals(l.prev) ? null : loadNode(l.prev);
		l.curr = NONE;
		l.prev = NONE; // help GC
		last = prev;
		saveLast();
		if (prev == null) {
			first = null;
			saveFirst();
		} else {
			prev.next = NONE;
			saveNode(prev);
		}
		deleteNode(curr);
		size--;
		saveSize();
		modCount++;
		return curr;
	}

	/**
	 * Unlinks non-null node x.
	 */
	Long unlink(Node x) {
		// assert x != null;
		final Long curr = x.curr;
		final Node next = NONE.equals(x.next) ? null : loadNode(x.next);
		final Node prev = NONE.equals(x.prev) ? null : loadNode(x.prev);

		if (prev == null) {
			first = next;
			saveFirst();
		} else {
			prev.next = next.curr;
			saveNode(prev);
			x.prev = NONE;
		}

		if (next == null) {
			last = prev;
			saveLast();
		} else {
			next.prev = prev.curr;
			saveNode(next);
			x.next = NONE;
		}

		deleteNode(curr);
		x.curr = NONE;
		size--;
		saveSize();
		modCount++;
		return curr;
	}

	/**
	 * Returns the first element in this list.
	 *
	 * @return the first element in this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public byte[] getFirst() {
		final Node f = first;
		if (f == null)
			throw new NoSuchElementException();
		return getValue(f.curr);
	}

	/**
	 * Returns the last element in this list.
	 *
	 * @return the last element in this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public byte[] getLast() {
		final Node l = last;
		if (l == null)
			throw new NoSuchElementException();
		return getValue(l.curr);
	}

	/**
	 * Removes and returns the first element from this list.
	 *
	 * @return the first element from this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public byte[] removeFirst() {
		final Node f = first;
		if (f == null)
			throw new NoSuchElementException();
		Long id = unlinkFirst(f);
		byte[] value = getValue(id);
		deleteValue(id);
		return value;
	}

	/**
	 * Removes and returns the last element from this list.
	 *
	 * @return the last element from this list
	 * @throws NoSuchElementException if this list is empty
	 */
	public byte[] removeLast() {
		final Node l = last;
		if (l == null)
			throw new NoSuchElementException();
		Long id = unlinkLast(l);
		byte[] value = getValue(id);
		deleteValue(id);
		return value;
	}

	/**
	 * Inserts the specified element at the beginning of this list.
	 *
	 * @param e the element to add
	 */
	public void addFirst(byte[] e) {
		Long id = id();
		putData(id, e);
		linkFirst(id);
	}

	/**
	 * Appends the specified element to the end of this list.
	 *
	 * <p>This method is equivalent to {@link #add}.
	 *
	 * @param e the element to add
	 */
	public void addLast(byte[] e) {
		Long id = id();
		putData(id, e);
		linkLast(id);
	}

	/**
	 * Returns {@code true} if this list contains the specified element.
	 * More formally, returns {@code true} if and only if this list contains
	 * at least one element {@code e} such that
	 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
	 *
	 * @param o element whose presence in this list is to be tested
	 * @return {@code true} if this list contains the specified element
	 */
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}

	/**
	 * Returns the number of elements in this list.
	 *
	 * @return the number of elements in this list
	 */
	public int size() {
		return size;
	}

	/**
	 * Appends the specified element to the end of this list.
	 *
	 * <p>This method is equivalent to {@link #addLast}.
	 *
	 * @param e element to be appended to this list
	 * @return {@code true} (as specified by {@link Collection#add})
	 */
	public boolean add(byte[] e) {
		Long id = id();
		putData(id, e);
		linkLast(id);
		return true;
	}

	/**
	 * Removes the first occurrence of the specified element from this list,
	 * if it is present.  If this list does not contain the element, it is
	 * unchanged.  More formally, removes the element with the lowest index
	 * {@code i} such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
	 * (if such an element exists).  Returns {@code true} if this list
	 * contained the specified element (or equivalently, if this list
	 * changed as a result of the call).
	 *
	 * @param o element to be removed from this list, if present
	 * @return {@code true} if this list contained the specified element
	 */
	public boolean remove(Object o) {
		if ((o instanceof byte[]) && o != null) {
			for (Node x = first; x != null; x = loadNode(x.next)) {
				if (Arrays.equals((byte[]) o, getValue(x.curr))) {
					Long id = unlink(x);
					deleteValue(id);
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Appends all of the elements in the specified collection to the end of
	 * this list, in the order that they are returned by the specified
	 * collection's iterator.  The behavior of this operation is undefined if
	 * the specified collection is modified while the operation is in
	 * progress.  (Note that this will occur if the specified collection is
	 * this list, and it's nonempty.)
	 *
	 * @param c collection containing elements to be added to this list
	 * @return {@code true} if this list changed as a result of the call
	 * @throws NullPointerException if the specified collection is null
	 */
	public boolean addAll(Collection<? extends byte[]> c) {
		return addAll(size, c);
	}

	/**
	 * Inserts all of the elements in the specified collection into this
	 * list, starting at the specified position.  Shifts the element
	 * currently at that position (if any) and any subsequent elements to
	 * the right (increases their indices).  The new elements will appear
	 * in the list in the order that they are returned by the
	 * specified collection's iterator.
	 *
	 * @param index index at which to insert the first element
	 *              from the specified collection
	 * @param c collection containing elements to be added to this list
	 * @return {@code true} if this list changed as a result of the call
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 * @throws NullPointerException if the specified collection is null
	 */
	@SuppressWarnings("unused")
	public boolean addAll(int index, Collection<? extends byte[]> c) {
		checkPositionIndex(index);

		Object[] a = c.toArray();
		int numNew = a.length;
		if (numNew == 0)
			return false;

		Node pred, succ;
		if (index == size) {
			succ = null;
			pred = last;
		} else {
			succ = node(index);
			pred = loadNode(succ.prev);
		}

		for (Object o : a) {
			byte[] e = (byte[]) o;
			Long id = id();
			putData(id, e);
			Node newNode = new Node(pred.curr, id, NONE);
			if (pred == null) {
				first = newNode;
				saveFirst();
			} else {
				pred.next = newNode.curr;
				saveNode(pred);
			}
			pred = newNode;
		}

		if (succ == null) {
			last = pred;
			saveLast();
		} else {
			pred.next = succ.curr;
			saveNode(pred);
			succ.prev = pred.curr;
			saveNode(succ);
		}

		size += numNew;
		modCount++;
		return true;
	}

	/**
	 * Removes all of the elements from this list.
	 * The list will be empty after this call returns.
	 */
	public void clear() {
		// Clearing all of the links between nodes is "unnecessary", but:
		// - helps a generational GC if the discarded nodes inhabit
		//   more than one generation
		// - is sure to free memory even if there is a reachable Iterator
		for (Node x = first; x != null;) {
			Node next = loadNode(x.next);
			deleteNode(x.curr);
			x.next = NONE;
			x.prev = NONE;
			x.curr = NONE;
			x = next;
		}
		first = last = null;
		saveFirst();
		saveLast();
		size = 0;
		modCount++;
	}

	// Positional Access Operations

	/**
	 * Returns the element at the specified position in this list.
	 *
	 * @param index index of the element to return
	 * @return the element at the specified position in this list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public byte[] get(int index) {
		checkElementIndex(index);
		return getValue(node(index).curr);
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 *
	 * @param index index of the element to replace
	 * @param element element to be stored at the specified position
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public byte[] set(int index, byte[] element) {
		checkElementIndex(index);
		Node x = node(index);
		byte[] oldVal = getValue(x.curr);
		putData(x.curr, element);
		return oldVal;
	}

	/**
	 * Inserts the specified element at the specified position in this list.
	 * Shifts the element currently at that position (if any) and any
	 * subsequent elements to the right (adds one to their indices).
	 *
	 * @param index index at which the specified element is to be inserted
	 * @param element element to be inserted
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public void add(int index, byte[] element) {
		checkPositionIndex(index);

		if (index == size) {
			Long id = id();
			putData(id, element);
			linkLast(id);
		} else {
			Long id = id();
			putData(id, element);
			linkBefore(id, node(index));
		}
	}

	/**
	 * Removes the element at the specified position in this list.  Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 * Returns the element that was removed from the list.
	 *
	 * @param index the index of the element to be removed
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 */
	public byte[] remove(int index) {
		checkElementIndex(index);
		Long id = unlink(node(index));
		byte[] value = getValue(id);
		deleteValue(id);
		return value;
	}

	/**
	 * Tells if the argument is the index of an existing element.
	 */
	private boolean isElementIndex(int index) {
		return index >= 0 && index < size;
	}

	/**
	 * Tells if the argument is the index of a valid position for an
	 * iterator or an add operation.
	 */
	private boolean isPositionIndex(int index) {
		return index >= 0 && index <= size;
	}

	/**
	 * Constructs an IndexOutOfBoundsException detail message.
	 * Of the many possible refactorings of the error handling code,
	 * this "outlining" performs best with both server and client VMs.
	 */
	private String outOfBoundsMsg(int index) {
		return "Index: " + index + ", Size: " + size;
	}

	private void checkElementIndex(int index) {
		if (!isElementIndex(index))
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
	}

	private void checkPositionIndex(int index) {
		if (!isPositionIndex(index))
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
	}

	/**
	 * Returns the (non-null) Node at the specified element index.
	 */
	Node node(int index) {
		// assert isElementIndex(index);

		if (index < (size >> 1)) {
			Node x = first;
			for (int i = 0; i < index; i++)
				x = loadNode(x.next);
			return x;
		} else {
			Node x = last;
			for (int i = size - 1; i > index; i--)
				x = loadNode(x.prev);
			return x;
		}
	}

	// Search Operations

	/**
	 * Returns the index of the first occurrence of the specified element
	 * in this list, or -1 if this list does not contain the element.
	 * More formally, returns the lowest index {@code i} such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
	 * or -1 if there is no such index.
	 *
	 * @param o element to search for
	 * @return the index of the first occurrence of the specified element in
	 *         this list, or -1 if this list does not contain the element
	 */
	public int indexOf(Object o) {
		int index = 0;
		if (o != null && (o instanceof byte[])) {
			for (Node x = first; x != null; x = loadNode(x.next)) {
				if (Arrays.equals((byte[]) o, getValue(x.curr)))
					return index;
				index++;
			}
		}
		return -1;
	}

	/**
	 * Returns the index of the last occurrence of the specified element
	 * in this list, or -1 if this list does not contain the element.
	 * More formally, returns the highest index {@code i} such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
	 * or -1 if there is no such index.
	 *
	 * @param o element to search for
	 * @return the index of the last occurrence of the specified element in
	 *         this list, or -1 if this list does not contain the element
	 */
	public int lastIndexOf(Object o) {
		int index = size;
		if (o != null && (o instanceof byte[])) {
			for (Node x = last; x != null; x = loadNode(x.prev)) {
				index--;
				if (Arrays.equals((byte[]) o, getValue(x.curr)))
					return index;
			}
		}
		return -1;
	}

	// Queue operations.

	/**
	 * Retrieves, but does not remove, the head (first element) of this list.
	 *
	 * @return the head of this list, or {@code null} if this list is empty
	 * @since 1.5
	 */
	public byte[] peek() {
		final Node f = first;
		return (f == null) ? null : getValue(f.curr);
	}

	/**
	 * Retrieves, but does not remove, the head (first element) of this list.
	 *
	 * @return the head of this list
	 * @throws NoSuchElementException if this list is empty
	 * @since 1.5
	 */
	public byte[] element() {
		return getFirst();
	}

	/**
	 * Retrieves and removes the head (first element) of this list.
	 *
	 * @return the head of this list, or {@code null} if this list is empty
	 * @since 1.5
	 */
	public byte[] poll() {
		final Node f = first;
		return (f == null) ? null : getValue(unlinkFirst(f));
	}

	/**
	 * Retrieves and removes the head (first element) of this list.
	 *
	 * @return the head of this list
	 * @throws NoSuchElementException if this list is empty
	 * @since 1.5
	 */
	public byte[] remove() {
		return removeFirst();
	}

	/**
	 * Adds the specified element as the tail (last element) of this list.
	 *
	 * @param e the element to add
	 * @return {@code true} (as specified by {@link Queue#offer})
	 * @since 1.5
	 */
	public boolean offer(byte[] e) {
		return add(e);
	}

	// Deque operations
	/**
	 * Inserts the specified element at the front of this list.
	 *
	 * @param e the element to insert
	 * @return {@code true} (as specified by {@link Deque#offerFirst})
	 * @since 1.6
	 */
	public boolean offerFirst(byte[] e) {
		addFirst(e);
		return true;
	}

	/**
	 * Inserts the specified element at the end of this list.
	 *
	 * @param e the element to insert
	 * @return {@code true} (as specified by {@link Deque#offerLast})
	 * @since 1.6
	 */
	public boolean offerLast(byte[] e) {
		addLast(e);
		return true;
	}

	/**
	 * Retrieves, but does not remove, the first element of this list,
	 * or returns {@code null} if this list is empty.
	 *
	 * @return the first element of this list, or {@code null}
	 *         if this list is empty
	 * @since 1.6
	 */
	public byte[] peekFirst() {
		final Node f = first;
		return (f == null) ? null : getValue(f.curr);
	}

	/**
	 * Retrieves, but does not remove, the last element of this list,
	 * or returns {@code null} if this list is empty.
	 *
	 * @return the last element of this list, or {@code null}
	 *         if this list is empty
	 * @since 1.6
	 */
	public byte[] peekLast() {
		final Node l = last;
		return (l == null) ? null : getValue(l.curr);
	}

	/**
	 * Retrieves and removes the first element of this list,
	 * or returns {@code null} if this list is empty.
	 *
	 * @return the first element of this list, or {@code null} if
	 *     this list is empty
	 * @since 1.6
	 */
	public byte[] pollFirst() {
		final Node f = first;
		return (f == null) ? null : getValue(unlinkFirst(f));
	}

	/**
	 * Retrieves and removes the last element of this list,
	 * or returns {@code null} if this list is empty.
	 *
	 * @return the last element of this list, or {@code null} if
	 *     this list is empty
	 * @since 1.6
	 */
	public byte[] pollLast() {
		final Node l = last;
		return (l == null) ? null : getValue(unlinkLast(l));
	}

	/**
	 * Pushes an element onto the stack represented by this list.  In other
	 * words, inserts the element at the front of this list.
	 *
	 * <p>This method is equivalent to {@link #addFirst}.
	 *
	 * @param e the element to push
	 * @since 1.6
	 */
	public void push(byte[] e) {
		addFirst(e);
	}

	/**
	 * Pops an element from the stack represented by this list.  In other
	 * words, removes and returns the first element of this list.
	 *
	 * <p>This method is equivalent to {@link #removeFirst()}.
	 *
	 * @return the element at the front of this list (which is the top
	 *         of the stack represented by this list)
	 * @throws NoSuchElementException if this list is empty
	 * @since 1.6
	 */
	public byte[] pop() {
		return removeFirst();
	}

	/**
	 * Removes the first occurrence of the specified element in this
	 * list (when traversing the list from head to tail).  If the list
	 * does not contain the element, it is unchanged.
	 *
	 * @param o element to be removed from this list, if present
	 * @return {@code true} if the list contained the specified element
	 * @since 1.6
	 */
	public boolean removeFirstOccurrence(Object o) {
		return remove(o);
	}

	/**
	 * Removes the last occurrence of the specified element in this
	 * list (when traversing the list from head to tail).  If the list
	 * does not contain the element, it is unchanged.
	 *
	 * @param o element to be removed from this list, if present
	 * @return {@code true} if the list contained the specified element
	 * @since 1.6
	 */
	public boolean removeLastOccurrence(Object o) {
		if (o != null && (o instanceof byte[])) {
			for (Node x = last; x != null; x = loadNode(x.prev)) {
				if (Arrays.equals((byte[]) o, getValue(x.curr))) {
					unlink(x);
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Returns a list-iterator of the elements in this list (in proper
	 * sequence), starting at the specified position in the list.
	 * Obeys the general contract of {@code List.listIterator(int)}.<p>
	 *
	 * The list-iterator is <i>fail-fast</i>: if the list is structurally
	 * modified at any time after the Iterator is created, in any way except
	 * through the list-iterator's own {@code remove} or {@code add}
	 * methods, the list-iterator will throw a
	 * {@code ConcurrentModificationException}.  Thus, in the face of
	 * concurrent modification, the iterator fails quickly and cleanly, rather
	 * than risking arbitrary, non-deterministic behavior at an undetermined
	 * time in the future.
	 *
	 * @param index index of the first element to be returned from the
	 *              list-iterator (by a call to {@code next})
	 * @return a ListIterator of the elements in this list (in proper
	 *         sequence), starting at the specified position in the list
	 * @throws IndexOutOfBoundsException {@inheritDoc}
	 * @see List#listIterator(int)
	 */
	public ListIterator<byte[]> listIterator(int index) {
		checkPositionIndex(index);
		return new ListItr(index);
	}

	private class ListItr implements ListIterator<byte[]> {
		private Node lastReturned;
		private Node next;
		private int nextIndex;
		private int expectedModCount = modCount;

		ListItr(int index) {
			// assert isPositionIndex(index);
			next = (index == size) ? null : node(index);
			nextIndex = index;
		}

		public boolean hasNext() {
			return nextIndex < size;
		}

		public byte[] next() {
			checkForComodification();
			if (!hasNext())
				throw new NoSuchElementException();

			lastReturned = next;
			next = loadNode(next.next);
			nextIndex++;
			return getValue(lastReturned.curr);
		}

		public boolean hasPrevious() {
			return nextIndex > 0;
		}

		public byte[] previous() {
			checkForComodification();
			if (!hasPrevious())
				throw new NoSuchElementException();

			lastReturned = next = (next == null) ? last : loadNode(next.prev);
			nextIndex--;
			return getValue(lastReturned.curr);
		}

		public int nextIndex() {
			return nextIndex;
		}

		public int previousIndex() {
			return nextIndex - 1;
		}

		public void remove() {
			checkForComodification();
			if (lastReturned == null)
				throw new IllegalStateException();

			Node lastNext = loadNode(lastReturned.next);
			unlink(lastReturned);
			if (next == lastReturned)
				next = lastNext;
			else
				nextIndex--;
			lastReturned = null;
			expectedModCount++;
		}

		public void set(byte[] e) {
			if (lastReturned == null)
				throw new IllegalStateException();
			checkForComodification();
			putData(lastReturned.curr, e);
		}

		public void add(byte[] e) {
			checkForComodification();
			lastReturned = null;
			if (next == null) {
				Long id = id();
				putData(id, e);
				linkLast(id);
			} else {
				Long id = id();
				putData(id, e);
				linkBefore(id, next);
			}
			nextIndex++;
			expectedModCount++;
		}

		public void forEachRemaining(Consumer<? super byte[]> action) {
			Objects.requireNonNull(action);
			while (modCount == expectedModCount && nextIndex < size) {
				action.accept(getValue(next.curr));
				lastReturned = next;
				next = loadNode(next.next);
				nextIndex++;
			}
			checkForComodification();
		}

		final void checkForComodification() {
			if (modCount != expectedModCount)
				throw new ConcurrentModificationException();
		}
	}

	private static class Node {
		Long curr;
		Long next;
		Long prev;

		Node(Long prev, Long curr, Long next) {
			this.curr = curr;
			this.next = next;
			this.prev = prev;
		}
	}

	/**
	 * @since 1.6
	 */
	public Iterator<byte[]> descendingIterator() {
		return new DescendingIterator();
	}

	/**
	 * Adapter to provide descending iterators via ListItr.previous
	 */
	private class DescendingIterator implements Iterator<byte[]> {
		private final ListItr itr = new ListItr(size());

		public boolean hasNext() {
			return itr.hasPrevious();
		}

		public byte[] next() {
			return itr.previous();
		}

		public void remove() {
			itr.remove();
		}
	}

	/**
	 * Returns an array containing all of the elements in this list
	 * in proper sequence (from first to last element).
	 *
	 * <p>The returned array will be "safe" in that no references to it are
	 * maintained by this list.  (In other words, this method must allocate
	 * a new array).  The caller is thus free to modify the returned array.
	 *
	 * <p>This method acts as bridge between array-based and collection-based
	 * APIs.
	 *
	 * @return an array containing all of the elements in this list
	 *         in proper sequence
	 */
	public Object[] toArray() {
		Object[] result = new Object[size];
		int i = 0;
		for (Node x = first; x != null; x = loadNode(x.next))
			result[i++] = getValue(x.curr);
		return result;
	}

	/**
	 * Returns an array containing all of the elements in this list in
	 * proper sequence (from first to last element); the runtime type of
	 * the returned array is that of the specified array.  If the list fits
	 * in the specified array, it is returned therein.  Otherwise, a new
	 * array is allocated with the runtime type of the specified array and
	 * the size of this list.
	 *
	 * <p>If the list fits in the specified array with room to spare (i.e.,
	 * the array has more elements than the list), the element in the array
	 * immediately following the end of the list is set to {@code null}.
	 * (This is useful in determining the length of the list <i>only</i> if
	 * the caller knows that the list does not contain any null elements.)
	 *
	 * <p>Like the {@link #toArray()} method, this method acts as bridge between
	 * array-based and collection-based APIs.  Further, this method allows
	 * precise control over the runtime type of the output array, and may,
	 * under certain circumstances, be used to save allocation costs.
	 *
	 * <p>Suppose {@code x} is a list known to contain only strings.
	 * The following code can be used to dump the list into a newly
	 * allocated array of {@code String}:
	 *
	 * <pre>
	 *     String[] y = x.toArray(new String[0]);</pre>
	 *
	 * Note that {@code toArray(new Object[0])} is identical in function to
	 * {@code toArray()}.
	 *
	 * @param a the array into which the elements of the list are to
	 *          be stored, if it is big enough; otherwise, a new array of the
	 *          same runtime type is allocated for this purpose.
	 * @return an array containing the elements of the list
	 * @throws ArrayStoreException if the runtime type of the specified array
	 *         is not a supertype of the runtime type of every element in
	 *         this list
	 * @throws NullPointerException if the specified array is null
	 */
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {
		if (a.length < size)
			a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
		int i = 0;
		Object[] result = a;
		for (Node x = first; x != null; x = loadNode(x.next))
			result[i++] = getValue(x.curr);

		if (a.length > size)
			a[size] = null;

		return a;
	}

	/**
	 * Creates a <em><a href="Spliterator.html#binding">late-binding</a></em>
	 * and <em>fail-fast</em> {@link Spliterator} over the elements in this
	 * list.
	 *
	 * <p>The {@code Spliterator} reports {@link Spliterator#SIZED} and
	 * {@link Spliterator#ORDERED}.  Overriding implementations should document
	 * the reporting of additional characteristic values.
	 *
	 * @implNote
	 * The {@code Spliterator} additionally reports {@link Spliterator#SUBSIZED}
	 * and implements {@code trySplit} to permit limited parallelism..
	 *
	 * @return a {@code Spliterator} over the elements in this list
	 * @since 1.8
	 */
	@Override
	public Spliterator<byte[]> spliterator() {
		return new LLSpliterator(this, -1, 0);
	}

	/** A customized variant of Spliterators.IteratorSpliterator */
	static final class LLSpliterator implements Spliterator<byte[]> {
		static final int BATCH_UNIT = 1 << 10; // batch array size increment
		static final int MAX_BATCH = 1 << 25; // max batch array size;
		final BinaryQueue list; // null OK unless traversed
		Node current; // current node; null until initialized
		int est; // size estimate; -1 until first needed
		int expectedModCount; // initialized when est set
		int batch; // batch size for splits

		LLSpliterator(BinaryQueue list, int est, int expectedModCount) {
			this.list = list;
			this.est = est;
			this.expectedModCount = expectedModCount;
		}

		final int getEst() {
			int s; // force initialization
			final BinaryQueue lst;
			if ((s = est) < 0) {
				if ((lst = list) == null)
					s = est = 0;
				else {
					expectedModCount = lst.modCount;
					current = lst.first;
					s = est = lst.size;
				}
			}
			return s;
		}

		public long estimateSize() {
			return (long) getEst();
		}

		public Spliterator<byte[]> trySplit() {
			Node p;
			int s = getEst();
			if (s > 1 && (p = current) != null) {
				int n = batch + BATCH_UNIT;
				if (n > s)
					n = s;
				if (n > MAX_BATCH)
					n = MAX_BATCH;
				Object[] a = new Object[n];
				int j = 0;
				do {
					a[j++] = list.getValue(p.curr);
				} while ((p = list.loadNode(p.next)) != null && j < n);
				current = p;
				batch = j;
				est = s - j;
				return Spliterators.spliterator(a, 0, j, Spliterator.ORDERED);
			}
			return null;
		}

		public void forEachRemaining(Consumer<? super byte[]> action) {
			Node p;
			int n;
			if (action == null)
				throw new NullPointerException();
			if ((n = getEst()) > 0 && (p = current) != null) {
				current = null;
				est = 0;
				do {
					byte[] e = list.getValue(p.curr);
					p = list.loadNode(p.next);
					action.accept(e);
				} while (p != null && --n > 0);
			}
			if (list.modCount != expectedModCount)
				throw new ConcurrentModificationException();
		}

		public boolean tryAdvance(Consumer<? super byte[]> action) {
			Node p;
			if (action == null)
				throw new NullPointerException();
			if (getEst() > 0 && (p = current) != null) {
				--est;
				byte[] e = list.getValue(p.curr);
				current = list.loadNode(p.next);
				action.accept(e);
				if (list.modCount != expectedModCount)
					throw new ConcurrentModificationException();
				return true;
			}
			return false;
		}

		public int characteristics() {
			return Spliterator.ORDERED | Spliterator.SIZED | Spliterator.SUBSIZED;
		}
	}

}
