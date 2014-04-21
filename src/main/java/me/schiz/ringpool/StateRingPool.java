package me.schiz.ringpool;

import java.util.concurrent.atomic.AtomicInteger;

public class StateRingPool<T> {
	protected ThreadLocal<Integer> localPointer;
	protected int capacity;
	protected int indexMask;
	protected Holder[] objects;

	public StateRingPool(int capacity) {
		this.capacity = capacity;
		this.indexMask = capacity - 1;
		objects = new Holder[capacity];
		for(int i=0;i<capacity;i++) {
			objects[i] = new Holder(null);
		}
	}

	public int acquire(int expect, int update) {
		int ptr = getLocalPointer();
		for(int j=0, i=0; j<capacity;i = (ptr + j++) & indexMask) {
			if(objects[i].value != null && objects[i].state != null)	{
				if(objects[i].state.get() == expect) {
					if (objects[i].state.compareAndSet(expect, update)) {
						localPointer.set(i);
						return i;
					}
				}
			}
		}
		return -1;
	}

	public T get(int ptr) {
		return (T)objects[ptr].value;
	}

	public boolean delete(int ptr, int state) {
		if(objects[ptr].state.get() == state) {
			if(objects[ptr].state.compareAndSet(state, -1)) {
				objects[ptr].value = null;
				objects[ptr].state.lazySet(0);
				return true;
			}
		}
		return false;
	}

	protected int getLocalPointer() {
		Integer ptr;
		if((ptr = localPointer.get()) == null) {
			ptr = (Thread.currentThread().hashCode() & indexMask);
			localPointer.set(ptr);
		}
		return ptr;
	}

	public boolean put(T value, int state) {
		int ptr = getLocalPointer();
		for(int j=0, i=0; j<capacity;i = (ptr + j++) & indexMask) {
			if(objects[i].value == null) {
				objects[i].value = value;
				objects[i].state.lazySet(state);
				localPointer.set(i);
				return true;
			}
		}
 		return false;
	}

	private class Holder<T> {
		private volatile T value;
		private volatile long p1, p2, p3, p4, p5, p6 = 6L;
		private AtomicInteger state;
		private volatile long q1, q2, q3, q4, q5, q6 = 6L;

		public Holder(T value) {
			state = new AtomicInteger(0);
			this.value = value;
		}
	}
}
