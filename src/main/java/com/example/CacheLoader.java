package com.example;

@FunctionalInterface
public interface CacheLoader <K,V>{
    V loadCache(K key);
}
