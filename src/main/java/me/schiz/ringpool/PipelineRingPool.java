package me.schiz.ringpool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PipelineRingPool<T> implements RingPool{
	protected int indexMask;
	protected int capacity;
	protected int pipeline;
	protected Holder[] objects;
	protected ThreadLocal<Integer> localPointer;

	@Override
	public int acquire() {
		int ptr = getLocalPointer();
		for(int j=0, i=0; j<capacity;i = (ptr + j++) & indexMask) {
			if(objects[i] != null && objects[i].pipeline_level.get() < pipeline)	{
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
		indexMask = capacity - 1;
		this.capacity = capacity;
		this.pipeline = pipeline;
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
		for(int i=(ptr+1) & indexMask; i!=ptr ; i=(i+1) & indexMask) {
			if(objects[i].value == null) {
				if(objects[i].pipeline_level.compareAndSet(0, pipeline)) {
					objects[i].value = value;
					objects[i].pipeline_level.addAndGet(-1*pipeline);
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

	@Override
	public boolean delete(int ptr) {
		if(objects[ptr].value != null) {
			if(objects[ptr].pipeline_level.compareAndSet(0, pipeline)) {
				objects[ptr].value = null;
				objects[ptr].pipeline_level.addAndGet(-1*pipeline);
				return true;
			}
		} else return true;
		return false;
	}

	public Map<String, Object> getStats() {
		HashMap<String, Object> stats = new HashMap<>();
		int level;
		long free_objects = 0L, busy_pipes = 0L, free_pipes = 0L, null_objects = 0L;
		for(int i = 0;i<capacity;i++) {
			level = objects[i].pipeline_level.get();
			if(level == 0L)	free_objects++;
			if(level > 0L)	busy_pipes += Math.min(level, pipeline);
			if(level < pipeline)	free_pipes += pipeline - level; //avoid overload of AtomicInteger
			if(objects[i].value == null)	null_objects++;
		}
		stats.put("free_objects", free_objects);
		stats.put("busy_objects", capacity - free_objects);
		stats.put("free_pipes", free_pipes);
		stats.put("busy_pipes", busy_pipes);
		stats.put("null_objects", null_objects);
		stats.put("notnull_objects", capacity - null_objects);
		return stats;
	}

	public int busyLevel(int ptr) {
		return Math.min(objects[ptr].pipeline_level.get(), pipeline);
	}

	private class Holder<T> {
		private volatile T value;
		private volatile long p1, p2, p3, p4, p5, p6 = 6L;
		private AtomicInteger pipeline_level;
		private volatile long q1, q2, q3, q4, q5, q6 = 6L;

		public Holder(T value) {
			this.value = value;
			pipeline_level = new AtomicInteger(0);
		}
	}
}
