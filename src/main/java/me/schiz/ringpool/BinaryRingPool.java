package me.schiz.ringpool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinaryRingPool<T> implements RingPool {
	protected int indexMask;
	protected int capacity;
	protected Holder[] objects;
	protected ThreadLocal<Integer> localPointer;

	public BinaryRingPool(int capacity) {
		this.capacity = capacity;
		indexMask = capacity - 1;
		objects = new Holder[capacity];
		localPointer = new ThreadLocal<>();

		for(int i=0;i<capacity;i++) {
			objects[i] = new Holder<T>(null);
		}
	}

	protected int getLocalPointer() {
		Integer ptr;
		if((ptr = localPointer.get()) == null) {
			ptr = (Thread.currentThread().hashCode() & indexMask);
			localPointer.set(ptr);
		}
		return ptr;
	}

	public boolean put(T value) {
		int ptr = getLocalPointer();
		for(int j=0, i=0; j<capacity;i = (ptr + j++) & indexMask) {
			if(objects[i].value == null) {
				if(objects[i].state.compareAndSet(Holder.FREE, Holder.BUSY)) {
					objects[i].value = value;
					objects[i].state.lazySet(Holder.FREE);
					localPointer.set(i);
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public int acquire() {
		int ptr = getLocalPointer();
		for(int j=0, i=0; j<capacity;i = (ptr + j++) & indexMask) {
			if(objects[i].value != null)	{
				if(objects[i].state.compareAndSet(Holder.FREE, Holder.BUSY)) {
					localPointer.set(i);
					return i;
				}
			}
		}
		return -1;
	}

	@Override
	public boolean release(int ptr) {
		return objects[ptr].state.compareAndSet(Holder.BUSY, Holder.FREE);
	}

	@Override
	public T get(int ptr) {
		return (T)objects[ptr].value;
	}

	@Override
	public boolean delete(int ptr) {
		if(objects[ptr].state.compareAndSet(Holder.FREE, Holder.BUSY)) {
			objects[ptr].value = null;
			objects[ptr].state.lazySet(Holder.FREE);
			return true;
		}
		return false;
	}

	public boolean delete(int ptr, boolean isAcquired) {
		if(!isAcquired) {
			return delete(ptr);
		}
		objects[ptr].value = null;
		objects[ptr].state.lazySet(Holder.FREE);
		return true;
	}

	public Map<String, Object> getStats() {
		HashMap<String, Object> stats = new HashMap<>();
		int free = 0, null_objects = 0;
		for(int i = 0;i<capacity;i++) {
			if(objects[i].state.get() == Holder.FREE)	free++;
			if(objects[i].value == null)	null_objects++;
		}
		stats.put("free", free);
		stats.put("busy", capacity - free);
		stats.put("null_objects", null_objects);
		stats.put("notnull_objects", capacity - null_objects);
		return stats;
	}

	public boolean isBusy(int ptr) {
		return objects[ptr].state.get();
	}

	private class Holder<T> {
		private final static boolean FREE = false;
		private final static boolean BUSY = true;
		//avoid false sharing
		private volatile long p1, p2, p3, p4, p5, p6 = 6L;
		private AtomicBoolean state = new AtomicBoolean(FREE);
		private volatile long q1, q2, q3, q4, q5, q6 = 6L;
		private volatile T value;
		public Holder(T value) {
			this.value = value;
		}
	}
}
