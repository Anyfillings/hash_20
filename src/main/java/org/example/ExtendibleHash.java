package org.example;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Extendible Hash (упрощённая реализация).
 * Доработки:
 *  - Сериализация для сохранения/загрузки
 *  - Методы saveToFile / loadFromFile
 *  - printStatus() для отладки (поддержка Main)
 *
 * Замечание: для сериализации K/V должны быть Serializable.
 */
public class ExtendibleHash<K, V> implements Serializable {
    private static final long serialVersionUID = 1L;

    // Директория хранит ссылки на бакеты; один и тот же бакет
    // может повторяться в нескольких слотах (по префиксу).
    private List<Bucket<K, V>> directory;
    private int globalDepth;
    private final int bucketSize;

    public ExtendibleHash(int bucketSize) {
        this.bucketSize = bucketSize;
        this.globalDepth = 1;
        this.directory = new ArrayList<>();
        // Изначально два бакета глубины 1
        Bucket<K, V> b0 = new Bucket<>(1, bucketSize);
        Bucket<K, V> b1 = new Bucket<>(1, bucketSize);
        directory.add(b0); // 0
        directory.add(b1); // 1
    }

    private int mask(int depth) {
        return (1 << depth) - 1;
    }

    private int indexForKey(Object keyHash) {
        // Берём hashCode и мапим по глобальной глубине
        int h = keyHash.hashCode();
        return h & mask(globalDepth);
    }

    public synchronized void put(K key, V value) {
        int idx = indexForKey(key);
        Bucket<K, V> bucket = directory.get(idx);

        if (!bucket.isFull() || bucket.containsKey(key)) {
            bucket.put(key, value);
            return;
        }

        // Сплитим, если бакет полон и ключ новый
        splitBucket(idx);
        // После сплита пробуем снова
        idx = indexForKey(key);
        directory.get(idx).put(key, value);
    }

    public synchronized V get(K key) {
        Bucket<K, V> b = directory.get(indexForKey(key));
        return b.get(key);
    }

    public synchronized V remove(K key) {
        Bucket<K, V> b = directory.get(indexForKey(key));
        return b.remove(key);
    }

    private void splitBucket(int bucketIndex) {
        Bucket<K, V> oldBucket = directory.get(bucketIndex);
        int oldLocalDepth = oldBucket.getLocalDepth();
        int newLocalDepth = oldLocalDepth + 1;

        // Если локальная глубина после сплита превысит глобальную — удваиваем директорию
        if (newLocalDepth > globalDepth) {
            doubleDirectory();
        }

        // Создаём новый бакет
        Bucket<K, V> newBucket = new Bucket<>(newLocalDepth, bucketSize);
        oldBucket.setLocalDepth(newLocalDepth);

        // Переназначаем слоты директории: все индексы, соответствующие новому биту,
        // должны указывать на новый бакет.
        int pattern = 1 << (newLocalDepth - 1);
        for (int i = 0; i < directory.size(); i++) {
            Bucket<K, V> b = directory.get(i);
            if (b == oldBucket) {
                if ((i & pattern) != 0) {
                    directory.set(i, newBucket);
                }
            }
        }

        // Ре-хешим элементы старого бакета между oldBucket и newBucket
        Map<K, V> items = oldBucket.getItems();
        // Очистить старые
        for (K k : items.keySet()) {
            oldBucket.remove(k);
        }
        for (Map.Entry<K, V> e : items.entrySet()) {
            int idx = indexForKey(e.getKey());
            directory.get(idx).put(e.getKey(), e.getValue());
        }
    }

    private void doubleDirectory() {
        int oldSize = directory.size();
        for (int i = 0; i < oldSize; i++) {
            directory.add(directory.get(i));
        }
        globalDepth++;
    }

    public int getGlobalDepth() {
        return globalDepth;
    }

    public int directorySize() {
        return directory.size();
    }

    // ------------------------
    //  Персистентность
    // ------------------------
    public void saveToFile(String path) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(path)))) {
            out.writeObject(this);
            out.flush();
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ExtendibleHash<K, V> loadFromFile(String path)
            throws IOException, ClassNotFoundException {
        Objects.requireNonNull(path, "path");
        try (ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(path)))) {
            return (ExtendibleHash<K, V>) in.readObject();
        }
    }

    // ------------------------
    //  Отладочный вывод состояния (для Main)
    // ------------------------
    /**
     * Печатает параметры таблицы и список уникальных бакетов:
     * их локальную глубину, какие слоты директории на них указывают и количество элементов.
     */
    public synchronized void printStatus() {
        System.out.println("ExtendibleHash status:");
        System.out.println("  globalDepth   = " + globalDepth);
        System.out.println("  directorySize = " + directory.size());

        // Сгруппировать слоты директории по уникальным объектам Bucket
        Map<Bucket<K, V>, List<Integer>> groups = new LinkedHashMap<>();
        for (int i = 0; i < directory.size(); i++) {
            Bucket<K, V> b = directory.get(i);
            groups.computeIfAbsent(b, k -> new ArrayList<>()).add(i);
        }

        int idx = 0;
        for (Map.Entry<Bucket<K, V>, List<Integer>> e : groups.entrySet()) {
            Bucket<K, V> b = e.getKey();
            int count = b.getItems().size();
            System.out.println("  bucket#" + idx
                    + " localDepth=" + b.getLocalDepth()
                    + " slots=" + e.getValue()
                    + " items=" + count);
            idx++;
        }
    }
}
