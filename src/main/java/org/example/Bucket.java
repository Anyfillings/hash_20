package org.example;

import java.util.HashMap;
import java.util.Map;

public class Bucket<K, V> {
    private final Map<K, V> items;
    private int localDepth;
    private final int size;

    public Bucket(int depth, int size) {
        this.localDepth = depth;
        this.size = size;
        this.items = new HashMap<>();
    }

    public boolean isFull() {
        return items.size() >= size;
    }

    public int getLocalDepth() {
        return localDepth;
    }

    public void incrementLocalDepth() {
        localDepth++;
    }

    public boolean insert(K key, V value) {
        if (isFull() && !items.containsKey(key)) return false;
        items.put(key, value);
        return true;
    }

    public V get(K key) {
        return items.get(key);
    }

    public V remove(K key) {
        return items.remove(key);
    }

    public Map<K, V> getItems() {
        return new HashMap<>(items);
    }

    public boolean containsKey(K key) {
        return items.containsKey(key);
    }

    @Override
    public String toString() {
        return "Bucket{depth=" + localDepth + ", items=" + items + "}";
    }
}
