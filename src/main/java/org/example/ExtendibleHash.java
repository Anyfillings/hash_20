package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Extendible Hash using per-bucket, per-mutation disk persistence via {@link ExtHashBucket}.
 *
 * API совместим с учебными заданиями: put/get/remove, printStatus, saveToFile/loadFromFile.
 * Сохранение бакетов на диск выполняется самим ExtHashBucket при каждой мутации.
 * Здесь мы сериализуем только "каркас" таблицы: глобальную глубину, параметры
 * и соответствие слотов каталога -> имена файлов бакетов.
 */
public class ExtendibleHash<K extends Serializable, V extends Serializable> implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---- параметры ----
    private final int bucketCapacity;
    private int globalDepth;

    // ---- каталог ----
    private transient Path storageDir;
    private List<ExtHashBucket<K,V>> directory; // длина = 2^globalDepth, элементы — ссылки на бакеты
    private final Map<String, ExtHashBucket<K,V>> uniqueBuckets = new LinkedHashMap<>();
    private int nextBucketId = 0;

    // ---------- ctors ----------

    /**
     * Удобный конструктор-обёртка: сохраняет совместимость со старыми вызовами new ExtendibleHash<>(depth)
     * и задаёт вместимость бакета по умолчанию (например, 4).
     */
    public ExtendibleHash(int initialGlobalDepth) {
        this(initialGlobalDepth, 4);
    }

    /** Удобный конструктор с дефолтной директорией хранения. */
    public ExtendibleHash(int initialGlobalDepth, int bucketCapacity) {
        this(initialGlobalDepth, bucketCapacity, Path.of("ext-hash-data"));
    }

    public ExtendibleHash(int initialGlobalDepth, int bucketCapacity, Path storageDir) {
        if (initialGlobalDepth < 0) throw new IllegalArgumentException("globalDepth < 0");
        this.bucketCapacity = bucketCapacity;
        this.globalDepth = initialGlobalDepth;
        this.storageDir = Objects.requireNonNull(storageDir, "storageDir");
        try { Files.createDirectories(storageDir); } catch (IOException ignored) {}

        int dirSize = 1 << globalDepth;
        this.directory = new ArrayList<>(Collections.nCopies(dirSize, null));

        // один исходный бакет на все слоты
        ExtHashBucket<K,V> b0 = newBucket(0);
        for (int i = 0; i < dirSize; i++) directory.set(i, b0);
    }

    private ExtHashBucket<K,V> newBucket(int localDepth) {
        String fileName = "bucket_" + (nextBucketId++) + ".bin";
        ExtHashBucket<K,V> b = new ExtHashBucket<>(localDepth, bucketCapacity, storageDir, fileName);
        uniqueBuckets.put(fileName, b);
        return b;
    }

    private int dirIndexForHash(int h) {
        int mask = (1 << globalDepth) - 1;
        return h & mask;
    }

    private int hash(Object key) {
        int h = key.hashCode();
        h ^= (h >>> 16);
        return h;
    }

    // ---------- операции ----------

    public V put(K key, V value) {
        int h = hash(key);
        int idx = dirIndexForHash(h);
        ExtHashBucket<K,V> bucket = directory.get(idx);

        if (!bucket.isFull() || bucket.containsKey(key)) {
            return bucket.put(key, value); // сам персистит
        }

        // need split; возможно понадобится увеличивать каталог
        split(idx);
        // после сплита вставим
        h = hash(key);
        idx = dirIndexForHash(h);
        return directory.get(idx).put(key, value);
    }

    public V get(K key) {
        int h = hash(key);
        ExtHashBucket<K,V> bucket = directory.get(dirIndexForHash(h));
        return bucket.get(key);
    }

    public V remove(K key) {
        int h = hash(key);
        ExtHashBucket<K,V> bucket = directory.get(dirIndexForHash(h));
        return bucket.remove(key); // сам персистит
    }

    public int sizeApprox() {
        // суммируем уникальные бакеты
        int sum = 0;
        for (ExtHashBucket<K,V> b : new LinkedHashSet<>(directory)) {
            sum += b.size();
        }
        return sum;
    }

    private void split(int dirIndex) {
        ExtHashBucket<K,V> oldB = directory.get(dirIndex);
        int oldDepth = oldB.getLocalDepth();

        // если локальная глубина == глобальной — расширяем каталог вдвое
        if (oldDepth == globalDepth) {
            int oldSize = 1 << globalDepth;
            directory.addAll(new ArrayList<>(directory)); // дублируем ссылки
            globalDepth++;
        }

        // новый бакет
        ExtHashBucket<K,V> newB = newBucket(oldDepth + 1);
        oldB.setLocalDepth(oldDepth + 1);

        // маска для определения, какой слот должен указывать на новый бакет
        int bit = 1 << oldDepth;
        int dirSize = 1 << globalDepth;

        for (int i = 0; i < dirSize; i++) {
            if (directory.get(i) == oldB && ((i & bit) != 0)) {
                directory.set(i, newB);
            }
        }

        // перераспределяем элементы
        Map<K,V> snapshot = oldB.snapshotItems();
        oldB.clear(); // персистится

        for (Map.Entry<K,V> e : snapshot.entrySet()) {
            K k = e.getKey();
            V v = e.getValue();
            int idx = dirIndexForHash(hash(k));
            directory.get(idx).put(k, v);
        }
    }

    // ---------- отладка ----------

    public void printStatus() {
        System.out.println("ExtendibleHash{globalDepth=" + globalDepth +
                ", uniqueBuckets=" + new LinkedHashSet<>(directory).size() +
                ", approxSize=" + sizeApprox() + "}");

        // сгруппируем слоты по объекту бакета
        Map<ExtHashBucket<K,V>, List<Integer>> groups = new LinkedHashMap<>();
        for (int i = 0; i < directory.size(); i++) {
            ExtHashBucket<K,V> b = directory.get(i);
            groups.computeIfAbsent(b, k -> new ArrayList<>()).add(i);
        }
        int idx = 0;
        for (Map.Entry<ExtHashBucket<K,V>, List<Integer>> e : groups.entrySet()) {
            ExtHashBucket<K,V> b = e.getKey();
            System.out.println("  bucket#" + idx
                    + " file=" + b.getFileName()
                    + " localDepth=" + b.getLocalDepth()
                    + " slots=" + e.getValue()
                    + " items=" + b.size());
            idx++;
        }
    }

    // ---------- сохранение/загрузка "каркаса" ----------

    public void saveToFile(String metaFilePath) throws IOException {
        // соберём уникальные бакеты и их имена
        Map<ExtHashBucket<K,V>, String> b2name = new IdentityHashMap<>();
        List<String> dirNames = new ArrayList<>(directory.size());

        for (ExtHashBucket<K,V> b : directory) {
            b2name.computeIfAbsent(b, __ -> b.getFileName());
            dirNames.add(b.getFileName());
        }

        Meta<K,V> meta = new Meta<>();
        meta.bucketCapacity = this.bucketCapacity;
        meta.globalDepth = this.globalDepth;
        meta.storageDir = this.storageDir.toString();
        meta.dirFileNames = dirNames;
        meta.nextBucketId = this.nextBucketId;

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metaFilePath))) {
            oos.writeObject(meta);
        }
    }

    public static <K extends Serializable, V extends Serializable>
    ExtendibleHash<K,V> loadFromFile(String metaFilePath) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(metaFilePath))) {
            @SuppressWarnings("unchecked")
            Meta<K,V> meta = (Meta<K,V>) ois.readObject();

            ExtendibleHash<K,V> h = new ExtendibleHash<>(meta.globalDepth, meta.bucketCapacity, Path.of(meta.storageDir));
            h.directory.clear();
            h.directory.addAll(Collections.nCopies(1 << h.globalDepth, null));
            h.uniqueBuckets.clear();
            h.nextBucketId = meta.nextBucketId;

            // загрузим/свяжем все бакеты, чтобы слоты ссылались на одни и те же объекты
            Map<String, ExtHashBucket<K,V>> cache = new HashMap<>();
            for (int i = 0; i < meta.dirFileNames.size(); i++) {
                String name = meta.dirFileNames.get(i);
                ExtHashBucket<K,V> b = cache.get(name);
                if (b == null) {
                    try {
                        b = ExtHashBucket.load(h.storageDir, name);
                        b.rebindStorageDir(h.storageDir);
                    } catch (ClassNotFoundException e) {
                        throw new IOException("Bucket class not found while loading", e);
                    }
                    cache.put(name, b);
                }
                h.directory.set(i, b);
                h.uniqueBuckets.put(name, b);
            }
            return h;
        } catch (ClassNotFoundException e) {
            throw new IOException("Meta class not found", e);
        }
    }

    // "каркас" состояния для сериализации
    private static class Meta<K,V> implements Serializable {
        private static final long serialVersionUID = 1L;
        int bucketCapacity;
        int globalDepth;
        String storageDir;
        List<String> dirFileNames;
        int nextBucketId;
    }
}
