package me.schiz.ringpool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

public class BlockingBinaryRingPool<T> extends BinaryRingPool<T> {
	protected Object monitor;
	protected volatile boolean notify;
	protected LongAdder fastAcq;
	protected LongAdder slowAcq;

	public final static int SLEEP = 10; //10ms

	public BlockingBinaryRingPool(int capacity) {
		super(capacity);
		fastAcq = new LongAdder();
		slowAcq = new LongAdder();
		notify = false;
		monitor = new Object();
	}

	public int acquire(long timeout, TimeUnit type) throws TimeoutException {
		//fast acquire
		int acq = super.acquire();
		if(acq != -1) {
			fastAcq.increment();
			return acq;
		}

		//slow acquire
		long start = System.nanoTime(), end;
		this.notify = true;
		while(acq == -1) {
			synchronized (monitor) {
				try {
					monitor.wait(SLEEP);
				} catch (InterruptedException e) {}
			}
			acq = super.acquire();
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
				throw new TimeoutException("BlockingBinaryRingPool acquire timeout " + delay);
			}
		}
		if(acq != -1)	slowAcq.increment();
		return acq;
	}

	@Override
	public int acquire() {
		try {
			return acquire(-1, TimeUnit.SECONDS); //without timeout
		} catch (TimeoutException e) {
			return acquire();
		}
	}

	@Override
	public boolean release(int ptr) {
		boolean r = super.release(ptr);
		if(this.notify == true) {
			synchronized (monitor) {
				this.monitor.notify();
			}
			this.notify = false;
		}
		return r;
	}

	public Map<String, Object> getStats() {
		HashMap<String, Object> stats = (HashMap<String, Object>)super.getStats();
		stats.put("fast_acq", fastAcq.longValue());
		stats.put("slow_acq", slowAcq.longValue());
		return stats;
	}
}
