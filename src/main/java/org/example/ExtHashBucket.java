package org.example;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A drop-in replacement bucket for the Extendible Hash that
 * persists itself to disk on EVERY content mutation
 * (put / remove / clear / setLocalDepth).
 *
 * IMPORTANT:
 *  - This class is intentionally separate from the original {@code Bucket}
 *    used by other hash implementations in the project.
 *  - Use this ONLY inside Extendible Hash.
 *  - Keys and values MUST be {@link Serializable}.
 *  - Writes are done atomically (temp-file + move) to avoid torn writes.
 *
 * Disk layout:
 *  Each bucket is serialized into its own file under {@code storageDir},
 *  e.g. {@code {storageDir}/bucket_{id}.bin}
 */
public class ExtHashBucket<K extends Serializable, V extends Serializable> implements Serializable {
    private static final long serialVersionUID = 1L;

    // ---- in-memory state ----
    private final LinkedHashMap<K, V> items = new LinkedHashMap<>();
    private int localDepth;
    private final int capacity;

    // ---- on-disk addressing ----
    private transient Path storageDir;
    private String fileName; // stable logical name, e.g. "bucket_3.bin"

    public ExtHashBucket(int localDepth, int capacity, Path storageDir, String fileName) {
        this.localDepth = localDepth;
        this.capacity = capacity;
        this.storageDir = Objects.requireNonNull(storageDir, "storageDir");
        this.fileName = Objects.requireNonNull(fileName, "fileName");
        persist(); // зафиксируем пустой бакет на диске
    }

    /** Rebind storage dir after deserialization if needed. */
    public void rebindStorageDir(Path storageDir) {
        this.storageDir = Objects.requireNonNull(storageDir, "storageDir");
    }

    public int getLocalDepth() {
        return localDepth;
    }

    public void setLocalDepth(int newDepth) {
        this.localDepth = newDepth;
        persist(); // persist depth change as well
    }

    public int capacity() {
        return capacity;
    }

    public boolean isFull() {
        return items.size() >= capacity;
    }

    public int size() {
        return items.size();
    }

    public boolean containsKey(K key) {
        return items.containsKey(key);
    }

    public V get(K key) {
        return items.get(key);
    }

    public Map<K, V> snapshotItems() {
        // a defensive copy for split/re-distribution logic
        return new LinkedHashMap<>(items);
    }

    /** Insert or update; returns previous value (if any). Always persists. */
    public V put(K key, V value) {
        V prev = items.put(key, value);
        persist();
        return prev;
    }

    /** Remove; returns previous value (if any). Always persists (even if no-op). */
    public V remove(K key) {
        V prev = items.remove(key);
        persist();
        return prev;
    }

    /** Remove everything; always persists. */
    public void clear() {
        items.clear();
        persist();
    }

    /** Change the target filename (used when creating a brand-new bucket on split). */
    public void setFileName(String newFileName) {
        this.fileName = Objects.requireNonNull(newFileName, "newFileName");
        persist();
    }

    public String getFileName() {
        return fileName;
    }

    // ---------- persistence ----------

    private Path filePath() {
        return storageDir.resolve(fileName);
    }

    private void persist() {
        try {
            // Write to a temp file then atomically move over the original file
            Path tmp = storageDir.resolve(fileName + ".tmp");
            Files.createDirectories(storageDir);

            try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(tmp))) {
                oos.writeObject(this.toSerializableForm());
            }
            Files.move(tmp, filePath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to persist bucket to " + filePath(), ioe);
        }
    }

    /**
     * Load the bucket from disk. You must call {@link #rebindStorageDir(Path)}
     * on the returned instance if you intend to persist changes under a new runtime.
     */
    @SuppressWarnings("unchecked")
    public static <K extends Serializable, V extends Serializable>
    ExtHashBucket<K, V> load(Path storageDir, String fileName) throws IOException, ClassNotFoundException {
        Path path = storageDir.resolve(fileName);
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(path))) {
            SerializedBucket<K, V> s = (SerializedBucket<K, V>) ois.readObject();
            ExtHashBucket<K, V> b = new ExtHashBucket<>(s.localDepth, s.capacity, storageDir, fileName);
            b.items.putAll(s.items);
            b.persist(); // гарантируем актуальное состояние на диске
            return b;
        }
    }

    // Keep the on-disk form decoupled to support transient fields.
    private SerializedBucket<K, V> toSerializableForm() {
        SerializedBucket<K, V> s = new SerializedBucket<>();
        s.localDepth = this.localDepth;
        s.capacity = this.capacity;
        s.items = new LinkedHashMap<>(this.items);
        return s;
    }

    /** A minimal serializable record of the bucket. */
    private static class SerializedBucket<K, V> implements Serializable {
        private static final long serialVersionUID = 1L;
        int localDepth;
        int capacity;
        LinkedHashMap<K, V> items;
    }

    @Override
    public String toString() {
        return "ExtHashBucket{depth=" + localDepth + ", size=" + items.size() +
                ", file='" + fileName + "'}";
    }
}
