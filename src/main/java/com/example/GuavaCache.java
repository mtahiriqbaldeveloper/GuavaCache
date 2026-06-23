package com.example;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class GuavaCache<K, V> implements AutoCloseable {

    private final int capacity;
    private final long expirationTime;
    private final Segment<K, V>[] segments;
    private final ReentrantLock[] stripReentrantLocks;
    private int concurrencyLevel = 16;
    private final ExecutorService executor;
    private final boolean isInternalExecutor;
    private final int segmentMask;
    private final int stripMask;
    private boolean useDefaultExecutor = false;


    public static class Builder<K, V> {
        private int capacity = 100;
        private long ttlMillis = TimeUnit.MINUTES.toMillis(10);
        private int concurrencyLevel = 16;
        private int stripLockCount = 64;
        private ExecutorService executor = Executors.newCachedThreadPool();
        private boolean useDefaultExecutor = true;


        public GuavaCache.Builder<K, V> capacity(int capacity) {
            if (capacity <= 0) throw new IllegalArgumentException("Capacity must be > 0");
            this.capacity = capacity;
            return this;
        }

        public GuavaCache.Builder<K, V> expireAfterWrite(long duration, TimeUnit unit) {
            if (duration <= 0) throw new IllegalArgumentException("TTL must be > 0");
            this.ttlMillis = unit.toMillis(duration);
            return this;
        }

        public GuavaCache.Builder<K, V> concurrencyLevel(int level) {
            this.concurrencyLevel = level;
            return this;
        }

        public Builder<K, V> executor(ExecutorService executor) {
            this.executor = executor;
            this.useDefaultExecutor = false;
            return this;
        }

        public GuavaCache.Builder<K, V> stripLockCount(int stripLockCount) {
            int count = 1;
            while (count < stripLockCount) count <<= 1;
            this.stripLockCount = count;
            return this;
        }

        public GuavaCache<K, V> build() {
            return new GuavaCache<>(this);
        }
    }


    @SuppressWarnings("unchecked")
    public GuavaCache(Builder<K, V> builder) {
        this.capacity = builder.capacity;
        this.expirationTime = builder.ttlMillis;
        this.concurrencyLevel = builder.concurrencyLevel;
        this.useDefaultExecutor = false;


        int sizee = 1;
        // make the size multiple of 2
        while (sizee < concurrencyLevel) sizee <<= 1;
        this.segments = new Segment[sizee];
        int eachSegmentSize = (capacity + sizee - 1) / sizee;
        for (int i = 0; i < sizee; i++) {
            segments[i] = new Segment<>(this.expirationTime, eachSegmentSize);
        }

        this.stripReentrantLocks = new ReentrantLock[builder.stripLockCount];
        for (int i = 0; i < stripReentrantLocks.length; i++) {
            stripReentrantLocks[i] = new ReentrantLock();
        }

        this.segmentMask = sizee - 1;
        this.stripMask = builder.stripLockCount - 1;

        if (builder.useDefaultExecutor) {
            this.executor = Executors.newCachedThreadPool();
            this.isInternalExecutor = true;
        } else {
            this.executor = builder.executor;
            this.isInternalExecutor = false;
        }
    }

    public CompletableFuture<V> getOrLoad(K key, CacheLoader<K, V> cacheLoader) {
        V cachedValue = get(key);
        if (cachedValue != null) {
            return CompletableFuture.completedFuture(cachedValue);
        }

        ReentrantLock lockForKey = getLockFor(key);
        lockForKey.lock();
        try {
            // Double-check
            cachedValue = get(key);
            if (cachedValue != null) {
                return CompletableFuture.completedFuture(cachedValue);
            }

            // Return the future WITHOUT calling .get()!
            CompletableFuture<V> future = CompletableFuture.supplyAsync(
                    () -> cacheLoader.loadCache(key), executor
            );

            // Chain operations to populate cache when complete
            future.thenAccept(v -> put(key, v));

            // Release lock IMMEDIATELY
            return future;  // 👈 Return to caller without blocking!

        } finally {
            lockForKey.unlock();  // 👈 Released at T2, not T5!
        }
    }

    private ReentrantLock getLockFor(K key) {
        int hashcode = spread(key.hashCode());
        return stripReentrantLocks[hashcode & stripMask];
    }

    private Segment<K, V> segmentFor(K key) {
        int hashCode = spread(key.hashCode());
        return segments[hashCode & segmentMask];
    }

    private static int spread(int h) {
        h ^= (h >>> 16) * 0x85ebca6b;
        h ^= (h >>> 13) * 0xc2b2ae35;
        return h & 0x7fffffff;
    }


    public V get(K key) {
        Segment<K, V> kvSegment = segmentFor(key);
        return kvSegment.get(key);
    }

    public void put(K key, V value) {
        Segment<K, V> kvSegment = segmentFor(key);
        kvSegment.put(key, value);
    }


    static class Node<K, V> {
        final K key;
        volatile V value;
        long expirationTime;

        Node<K, V> next;
        Node<K, V> prev;

        public Node(K key, V value, long ttlMillis) {
            this.key = key;
            this.value = value;
            this.expirationTime = System.currentTimeMillis() + ttlMillis;
        }

    }


    @Override
    public void close() {
        if (isInternalExecutor) {
            executor.shutdown();
        }
    }

}

