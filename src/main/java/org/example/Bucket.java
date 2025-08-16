package org.example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Бакет для extendible hash.
 * Минимально изменён: добавлена сериализация.
 */
public class Bucket<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<K, V> items;
    private int localDepth;
    private final int size;

    public Bucket(int depth, int size) {
        this.localDepth = depth;
        this.size = size;
        this.items = new HashMap<>();
    }

    public int getLocalDepth() {
        return localDepth;
    }

    public void setLocalDepth(int localDepth) {
        this.localDepth = localDepth;
    }

    public int getSize() {
        return size;
    }

    public boolean isFull() {
        return items.size() >= size;
    }

    public boolean put(K key, V value) {
        boolean existed = items.containsKey(key);
        items.put(key, value);
        return existed;
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
