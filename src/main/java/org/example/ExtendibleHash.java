package org.example;

import java.util.ArrayList;
import java.util.List;

public class ExtendibleHash<K, V> {
    private List<Bucket<K, V>> directory;
    private int globalDepth;
    private final int bucketSize;

    public ExtendibleHash(int bucketSize) {
        this.bucketSize = bucketSize;
        this.globalDepth = 1;
        this.directory = new ArrayList<>();
        for (int i = 0; i < (1 << globalDepth); i++) {
            directory.add(new Bucket<>(globalDepth, bucketSize));
        }
    }

    private int hash(K key) {
        return key.hashCode() & ((1 << globalDepth) - 1);
    }

    public void put(K key, V value) {
        int hash = hash(key);
        Bucket<K, V> bucket = directory.get(hash);

        if (bucket.insert(key, value)) return;

        while (!bucket.insert(key, value)) {
            if (bucket.getLocalDepth() == globalDepth) {
                doubleDirectory();
            }
            splitBucket(hash);
            hash = hash(key);
            bucket = directory.get(hash);
        }
    }

    public V get(K key) {
        return directory.get(hash(key)).get(key);
    }

    public void remove(K key) {
        directory.get(hash(key)).remove(key);
    }

    private void doubleDirectory() {
        List<Bucket<K, V>> newDirectory = new ArrayList<>(directory);
        newDirectory.addAll(directory);
        globalDepth++;
        directory = newDirectory;
    }

    private void splitBucket(int index) {
        Bucket<K, V> oldBucket = directory.get(index);
        int oldDepth = oldBucket.getLocalDepth();
        oldBucket.incrementLocalDepth();

        Bucket<K, V> newBucket = new Bucket<>(oldDepth + 1, bucketSize);
        int bit = 1 << oldDepth;

        for (int i = 0; i < directory.size(); i++) {
            if ((i & bit) != 0 && directory.get(i) == oldBucket) {
                directory.set(i, newBucket);
            }
        }

        for (var entry : oldBucket.getItems().entrySet()) {
            directory.get(hash(entry.getKey())).insert(entry.getKey(), entry.getValue());
        }

        oldBucket.getItems().clear();
    }

    public void printStatus() {
        System.out.println("Global depth: " + globalDepth);
        for (int i = 0; i < directory.size(); i++) {
            System.out.println("Index " + i + ": " + directory.get(i));
        }
    }
}
