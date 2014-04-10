package me.schiz.ringpool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinaryRingPool<T> implements RingPool {
	protected int capacity;
	protected Holder[] objects;
	protected ThreadLocal<Integer> localPointer;

	public BinaryRingPool(int capacity) {
		this.capacity = capacity;
		objects = new Holder[capacity];
		localPointer = new ThreadLocal<Integer>();

		for(int i=0;i<capacity;i++) {
			objects[i] = new Holder<T>(null);
		}
	}

	protected int getLocalPointer() {
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
		return objects[ptr].state.compareAndSet(Holder.BUSY, Holder.FREE);
	}

	@Override
	public T get(int ptr) {
		return (T)objects[ptr].value;
	}

	@Override
	public boolean delete(int ptr, boolean isAcquired) {
		if(!isAcquired) {
			isAcquired = objects[ptr].state.compareAndSet(Holder.FREE, Holder.BUSY);
			if(!isAcquired)	return false;
		}
		objects[ptr].value = null;
		objects[ptr].state.set(Holder.FREE);
		return true;
	}

	public Map<String, Object> getStats() {
		HashMap<String, Object> stats = new HashMap<String, Object>();
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
		private volatile T value;
		public final static boolean FREE = false;
		public final static boolean BUSY = true;
		private AtomicBoolean state = new AtomicBoolean(FREE);

		public Holder(T value) {
			this.value = value;
		}
	}
}
