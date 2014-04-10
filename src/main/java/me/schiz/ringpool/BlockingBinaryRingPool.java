package me.schiz.ringpool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

public class BlockingBinaryRingPool<T> extends BinaryRingPool<T> {
	protected Object ar_monitor;	//monitor for acquire/release
	protected Object pd_monitor;	//monitor for put/delete
	protected volatile boolean ar_notify;
	protected volatile boolean pd_notify;
	protected LongAdder fastAcq;
	protected LongAdder slowAcq;

	public final static int SLEEP = 10; //10ms

	public BlockingBinaryRingPool(int capacity) {
		super(capacity);
		fastAcq = new LongAdder();
		slowAcq = new LongAdder();
		ar_monitor = new Object();
		ar_notify = false;
		pd_monitor = new Object();
		pd_notify = false;
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
		this.ar_notify = true;
		while(acq == -1) {
			synchronized (ar_monitor) {
				try {
					ar_monitor.wait(SLEEP);
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
		if(this.ar_notify == true) {
			synchronized (ar_monitor) {
				this.ar_monitor.notify();
			}
			this.ar_notify = false;
		}
		return r;
	}

	public boolean put(T value, long timeout, TimeUnit type) throws TimeoutException {
		boolean res = super.put(value);
		if(res == true)	return true;

		long start = System.nanoTime(), end;
		pd_notify = true;
		while(res == false) {
			synchronized (pd_monitor) {
				try {
					pd_monitor.wait(SLEEP);
				} catch (InterruptedException e) {}
			}
			res = super.put(value);
			if(timeout < 0L)	continue;
			end = System.nanoTime();
			if(res == false && end - start >= type.toNanos(timeout)) {
				String delay;
				if(end - start > 1000000L) {
					delay = String.valueOf((end - start) / 1000000L);
					delay = delay + "ms";
				} else {
					delay = String.valueOf((end - start) / 1000L);
					delay = delay + "mks";
				}
				throw new TimeoutException("BlockingBinaryRingPool put timeout " + delay);
			}
		}
		return res;
	}

	@Override
	public boolean put(T value) {
		boolean res = false;
		try{
			res = put(value, -1, TimeUnit.SECONDS);
		} catch (TimeoutException e) { }
		return res;
	}

	@Override
	public boolean delete(int ptr, boolean isAcquired) {
		boolean rc = super.delete(ptr, isAcquired);
		if(rc && this.pd_notify == true) {
			synchronized (ar_monitor) {
				this.ar_monitor.notify();
			}
			this.pd_notify = false;
		}
		return rc;
	}

	public Map<String, Object> getStats() {
		HashMap<String, Object> stats = (HashMap<String, Object>)super.getStats();
		stats.put("fast_acq", fastAcq.longValue());
		stats.put("slow_acq", slowAcq.longValue());
		return stats;
	}
}
