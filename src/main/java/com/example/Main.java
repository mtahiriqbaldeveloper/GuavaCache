package com.example;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {

        GuavaCache<String, String> cache = new GuavaCache.Builder<String, String>()
                .capacity(100)
                .expireAfterWrite(5, TimeUnit.SECONDS)
                .concurrencyLevel(4)
                .stripLockCount(64)
                .build();

        CacheLoader<String, String> cacheLoader = key -> {
            try {
                System.out.println("  -> [DB QUERY] Fetching: " + key + " on thread: " + Thread.currentThread().getName());
                Thread.sleep(500);
                return "Data_" + key;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CacheLoadingException("loading from db got something wrong : " + e.getMessage());
            }
        };
        cache.put("mac","value1");
        System.out.println("Fetching A...");
        System.out.println("Result: " + cache.getOrLoad("A", cacheLoader));

        System.out.println("Fetching A again...");
        System.out.println("Result: " + cache.getOrLoad("A", cacheLoader));
        CompletableFuture<String> mac = cache.getOrLoad("mac", cacheLoader);
        mac.thenAccept(System.out::println);
        cache.getOrLoad("mac",cacheLoader).thenAccept(System.out::println);
        cache.close();
    }

}