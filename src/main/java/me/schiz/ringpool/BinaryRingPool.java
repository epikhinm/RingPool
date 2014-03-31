package me.schiz.ringpool;

import java.util.concurrent.atomic.AtomicBoolean;

public class BinaryRingPool<T> implements RingPool {
	private int capacity;
	private Holder[] objects;
	private ThreadLocal<Integer> localPointer;

	public BinaryRingPool(int capacity) {
		this.capacity = capacity;
		objects = new Holder[capacity];
		localPointer = new ThreadLocal<Integer>();

		for(int i=0;i<capacity;i++) {
			objects[i] = new Holder<T>(null);
		}
	}

	private int getLocalPointer() {
		Integer ptr;
		if((ptr = localPointer.get()) == null) {
			ptr = (int)(Thread.currentThread().hashCode() % capacity);
			localPointer.set(ptr);
		}
		return ptr;
	}

	public boolean put(T value) {
		int ptr = getLocalPointer();
		for(int i=(ptr+1)%this.capacity; i!=ptr ; i=(i+1)%this.capacity) {
			if(objects[i].value == null) {
				if(objects[i].state.compareAndSet(Holder.FREE, Holder.BUSY)) {
					objects[i].value = value;
					objects[i].state.set(Holder.FREE);
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
		for(int i=(ptr+1)%this.capacity; i!=ptr ; i=(i+1)%this.capacity) {
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
		return objects[ptr].state.weakCompareAndSet(Holder.BUSY, Holder.FREE);
	}

	@Override
	public T get(int ptr) {
		return (T)objects[ptr].value;
	}

	public void destroy(int ptr) {
		objects[ptr].value = null;
	}

	public Stats getStats() {
		Stats stats = new Stats();
		for(int i = 0;i<capacity;i++) {
			if(objects[i].state.get() == Holder.FREE)	stats.free_objects++;
			if(objects[i].value == null)	stats.null_objects++;
		}
		stats.busy_objects = capacity - stats.free_objects;
		stats.notnull_objects = capacity - stats.null_objects;
		return stats;
	}

	public boolean isBusy(int ptr) {
		return objects[ptr].state.get();
	}

	public class Stats{
		public int null_objects;
		public int notnull_objects;
		public int free_objects;
		public int busy_objects;
	}

	private class Holder<T> {
		private volatile T value;
		public final static boolean FREE = false;
		public final static boolean BUSY = true;
		private AtomicBoolean state = new AtomicBoolean(FREE);

		public Holder(T value) {
			this.value = value;
		}
	}
}
