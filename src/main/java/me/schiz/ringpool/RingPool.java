package me.schiz.ringpool;

public interface RingPool<T> {
	public int acquire();
	public boolean release(int ptr);
	public T get(int ptr);
	public boolean delete(int ptr, boolean isAcquired);
}
