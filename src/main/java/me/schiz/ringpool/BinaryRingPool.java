package me.schiz.ringpool;

import java.util.concurrent.atomic.AtomicBoolean;

public class BinaryRingPool<T> {
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
			ptr = (int)(Thread.currentThread().hashCode() % objects.length);
			localPointer.set(ptr);
		}
		return ptr;
	}

	public boolean tryPut(T value) {
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

	public int acquire() {
		int ptr = getLocalPointer();
		for(int i=(ptr+1)%this.capacity; i!=ptr ; i=(i+1)%this.capacity) {
			if(objects[i] != null)	{
				if(objects[i].state.compareAndSet(Holder.FREE, Holder.BUSY)) {
					localPointer.set(i);
					return i;
				}
			}
		}
		return -1;
	}

	public boolean release(int ptr) {
		if(objects[ptr].state.compareAndSet(Holder.BUSY, Holder.FREE)) {
			return true;
		}
		return false;
	}

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
		stats.notnull_object = capacity - stats.null_objects;
		return stats;
	}

	public class Stats{
		int null_objects;
		int notnull_object;
		int free_objects;
		int busy_objects;
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
