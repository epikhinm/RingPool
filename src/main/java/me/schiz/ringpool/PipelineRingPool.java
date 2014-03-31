package me.schiz.ringpool;

import java.util.concurrent.atomic.AtomicInteger;

public class PipelineRingPool<T> implements RingPool{
	private int capacity;
	private int pipeline;
	private Holder[] objects;
	private ThreadLocal<Integer> localPointer;

	@Override
	public int acquire() {
		int ptr = getLocalPointer();
		for(int i=(ptr+1)%this.capacity; i!=ptr ; i=(i+1)%this.capacity) {
			if(objects[i] != null)	{
				if(objects[i].pipeline_level.incrementAndGet() < pipeline) {
					localPointer.set(i);
					return i;
				} else {
					objects[i].pipeline_level.decrementAndGet();
				}
			}
		}
		return -1;
	}

	public PipelineRingPool(int capacity, int pipeline) {
		this.capacity = capacity;
		this.pipeline = pipeline;
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
				if(objects[i].pipeline_level.compareAndSet(0, pipeline)) {
					objects[i].value = value;
					objects[i].pipeline_level.set(0);
					localPointer.set(i);
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public boolean release(int ptr) {
		objects[ptr].pipeline_level.decrementAndGet();
		return true;
	}

	@Override
	public Object get(int ptr) {
		return (T)objects[ptr].value;
	}

	public Stats getStats() {
		Stats stats = new Stats();
		int level;
		for(int i = 0;i<capacity;i++) {
			level = objects[i].pipeline_level.get();
			if(level == 0)	stats.free_objects++;
			if(level > 0)	stats.busy_pipes += Math.min(level, pipeline);
			if(objects[i].value == null)	stats.null_objects++;
		}
		stats.busy_objects = capacity - stats.free_objects;
		stats.free_pipes = pipeline*capacity - stats.busy_pipes;
		stats.notnull_objects = capacity - stats.null_objects;
		return stats;
	}

	public int busyLevel(int ptr) {
		return Math.min(objects[ptr].pipeline_level.get(), pipeline);
	}

	public class Stats{
		public int null_objects;
		public int notnull_objects;
		public int free_objects;
		public int busy_objects;
		public int free_pipes;
		public int busy_pipes;
	}

	private class Holder<T> {
		private volatile T value;
		private AtomicInteger pipeline_level;

		public Holder(T value) {
			this.value = value;
			pipeline_level = new AtomicInteger(0);
		}
	}
}
