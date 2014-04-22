package me.schiz.ringpool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingStateRingPool<T> extends StateRingPool<T> {
	protected ConcurrentHashMap<Integer, AtomicInteger>	counters;
	public final static int SLEEP = 1; //1ms

	public BlockingStateRingPool(int capacity) {
		super(capacity);
		counters = new ConcurrentHashMap<>();
	}

	@Override
	public int acquire(int expect, int update) {
		try {
			return acquire(expect, update, -1, TimeUnit.SECONDS); //without timeout
		} catch (TimeoutException e) {
			return acquire(expect, update);
		}
	}

	public int acquire(int expect, int update, int timeout, TimeUnit type) throws TimeoutException {
 		//fast acquire
		int acq = acquire0(expect, update);
		if(acq != -1)	return acq;

		//slow acquire
		AtomicInteger expectCounter = counters.get(expect);
		if(expectCounter == null){
			expectCounter = new AtomicInteger(0);
			if(counters.putIfAbsent(expect, expectCounter) != null) {
				expectCounter = counters.get(expect);
			}
		}

		long start = System.nanoTime(), end;
	  	while(acq == -1) {
			synchronized (expectCounter) {
				try {
					expectCounter.wait(SLEEP);
				} catch (InterruptedException e) {}
			}
			acq = acquire0(expect, update);
			if(acq != -1)	return acq;
			if(timeout < 0L)	continue;
			end = System.nanoTime();
			if(acq == -1 && end - start >= type.toNanos(timeout)) {
				String delay;
				if(end - start > 1000000L) {
					delay = String.valueOf((end - start) /1000000L);
					delay = delay + "ms";
				} else {
					delay = String.valueOf((end - start) /1000L);
					delay = delay + "mks";
				}
				throw new TimeoutException("BlockingStateRingPool acquire timeout " + delay);
			}

		}
		return acq;
	}

	protected int acquire0(int expect, int update) {
		int tid = super.acquire(expect, update);
		if(tid >= 0) {
			decr(expect);
			incr(update);
		}
		return tid;
	}

	@Override
	public boolean delete(int ptr, int state) {
		return delete0(ptr, state);
	}

	public boolean delete0(int ptr, int state) {
		boolean success = super.delete(ptr, state);
		if(success)	decr(state);
		return success;
	}

	public boolean put0(T value, int state) {
		boolean success = super.put(value, state);
		if(success)	incr(state);
		return success;
	}

	public boolean release0(int ptr, int expect, int update) {
		boolean success = super.release(ptr, expect, update);
		if(success) {
			decr(expect);
			incr(update);
		}
		return success;
	}

	public int release(int ptr, int update) {
		int prev = super.release(ptr, update);
		decr(prev);
		incr(update);
		return prev;
	}

	protected void incr(int state) {
		AtomicInteger ai = counters.get(state);
		int prev = -1;
		if(ai != null){
			prev = ai.incrementAndGet();
		} else {
			ai = new AtomicInteger(1);
			if(counters.putIfAbsent(state, ai) != null) {
				ai = counters.get(state);
				prev = ai.incrementAndGet();
			}
		}
		if(prev < 1)	{
			synchronized (ai) {
				ai.notify();
			}
		}
	}

	protected void decr(int state) {
		AtomicInteger ai = counters.get(state);
		if(ai != null){
			ai.decrementAndGet();
		} else {
			ai = new AtomicInteger(-1);
			if(counters.putIfAbsent(state, ai) != null) {
				ai = counters.get(state);
				ai.decrementAndGet();
			}
		}
	}

	@Override
	protected boolean fn(T value) {
		return super.fn(value);
	}

	@Override
	public int getState(int ptr) {
		return super.getState(ptr);
	}
}
