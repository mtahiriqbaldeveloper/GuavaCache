package com.example;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Segment<K, V> {
    private Map<K, GuavaCache.Node<K, V>> table = new HashMap<>();
    private final GuavaCache.Node<K, V> head = new GuavaCache.Node<>(null, null, 0);
    private final GuavaCache.Node<K, V> tail = new GuavaCache.Node<>(null, null, 0);
    private final ConcurrentLinkedQueue<GuavaCache.Node<K, V>> nodeConcurrentLinkedQueue = new ConcurrentLinkedQueue<>();
    private final ReentrantReadWriteLock reentrantLock = new ReentrantReadWriteLock();

    private long ttlMilli;
    private int segmentCapacity;
    private int size = 0;

    public Segment(long ttlMilli, int segmentCapacity) {
        this.ttlMilli = ttlMilli;
        this.segmentCapacity = segmentCapacity;
        head.next = tail;
        tail.prev = head;
    }

    void put(K key, V value) {
        reentrantLock.writeLock().lock();
        try {
            drainRecencyQueue();
            removeExpired();
            GuavaCache.Node<K, V> node = table.get(key);
            if (node != null) {
                node.value = value;
                moveToHead(node);
                node.expirationTime = System.currentTimeMillis() + ttlMilli;
            } else {
                GuavaCache.Node<K, V> insertNode = new GuavaCache.Node<>(key, value, ttlMilli);
                table.put(key, insertNode);
                addToHead(insertNode);
                insertNode.expirationTime = System.currentTimeMillis() + ttlMilli;
                size++;
                if (size > this.segmentCapacity) {
                    GuavaCache.Node<K, V> removed = removeFromTail();
                    table.remove(removed.key);
                }
            }
        } finally {
            reentrantLock.writeLock().unlock();
        }
    }

    V get(K key) {
        reentrantLock.readLock().lock();
        try {
            if (table.containsKey(key)) {
                GuavaCache.Node<K, V> node = table.get(key);
                nodeConcurrentLinkedQueue.add(node);
                return node.value;
            }
        } finally {
            reentrantLock.readLock().unlock();
        }
        return null;
    }

    private void removeExpired() {
        Iterator<Map.Entry<K, GuavaCache.Node<K, V>>> iterator = table.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<K, GuavaCache.Node<K, V>> entry = iterator.next();
            if (System.currentTimeMillis() > entry.getValue().expirationTime) {
                removeNode(entry.getValue());
                iterator.remove();
            }
        }
    }

    private void drainRecencyQueue() {
        GuavaCache.Node<K, V> node;
        while ((node = nodeConcurrentLinkedQueue.poll()) != null) {
            if (table.containsKey(node.key)) {
                System.out.println("removed");
                moveToHead(node);
            }
        }
    }

    private void moveToHead(GuavaCache.Node<K, V> node) {
        removeNode(node);
        addToHead(node);
    }

    private void removeNode(GuavaCache.Node<K, V> node) {
        node.next.prev = node.prev;
        node.prev.next = node.next;
    }

    private void addToHead(GuavaCache.Node<K, V> node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private GuavaCache.Node<K, V> removeFromTail() {
        GuavaCache.Node<K, V> toRemove = tail.prev;
        removeNode(toRemove);
        return toRemove;
    }
}
