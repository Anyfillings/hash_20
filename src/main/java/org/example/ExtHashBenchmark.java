package org.example;

import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Бенчмарки extendible hash.
 * Сценарии:
 *  - insert10k: только вставка
 *  - insertThenRead10k: вставка + чтение из памяти
 *  - saveReadyHash10k: СЕРИАЛИЗАЦИЯ на диск заранее заполненной таблицы (измеряет "save")
 *  - loadFromDisk10k: ДЕСЕРИАЛИЗАЦИЯ с диска + чтение N ключей (измеряет "load")
 *  - insertSaveLoad10k: конец-в-конец: вставка -> save -> load -> валидация нескольких ключей
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(1)
public class ExtHashBenchmark {

    // Требование: ровно 10_000 операций
    private static final int N = 10_000;
    private static final int BUCKET_SIZE = 4;

    /* ------------------------- In-memory baseline ------------------------- */

    @State(Scope.Thread)
    public static class HashState {
        ExtendibleHash<Integer, Integer> hash;

        @Setup(Level.Invocation)
        public void setUp() {
            hash = new ExtendibleHash<>(BUCKET_SIZE);
        }
    }

    @Benchmark
    public void insert10k(HashState s) {
        for (int i = 0; i < N; i++) {
            s.hash.put(i, i);
        }
    }

    @Benchmark
    public int insertThenRead10k(HashState s) {
        for (int i = 0; i < N; i++) {
            s.hash.put(i, i);
        }
        int acc = 0;
        for (int i = 0; i < N; i++) {
            Integer v = s.hash.get(i);
            acc += (v == null ? 0 : v);
        }
        return acc;
    }

    /* ------------------------- Save-only benchmark ------------------------ */

    /**
     * Готовим заранее заполненную таблицу на каждую инвокацию —
     * бенчмарку остаётся только "saveToFile".
     */
    @State(Scope.Thread)
    public static class ReadyHashState {
        ExtendibleHash<Integer, Integer> hash;

        @Setup(Level.Invocation)
        public void setUp() {
            hash = new ExtendibleHash<>(BUCKET_SIZE);
            for (int i = 0; i < N; i++) {
                hash.put(i, i);
            }
        }
    }

    @Benchmark
    public void saveReadyHash10k(ReadyHashState s) throws IOException {
        Path tmp = Files.createTempFile("exthash_bench_save_", ".dat");
        try {
            s.hash.saveToFile(tmp.toString());
            // Замечание: файловая система может кешировать запись — это нормально для JMH.
            // При необходимости можно добавить fsync в saveToFile, но мы сохраняем минимальные изменения.
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    /* ------------------------- Load-only benchmark ------------------------ */

    /**
     * На каждую ИТЕРАЦИЮ готовим файл на диске с 10k элементами,
     * чтобы в самом бенчмарке измерять только "load + чтение".
     */
    @State(Scope.Thread)
    public static class PersistedFileState {
        Path tmpFile;

        @Setup(Level.Iteration)
        public void prepareFile() throws Exception {
            tmpFile = Files.createTempFile("exthash_bench_load_", ".dat");
            ExtendibleHash<Integer, Integer> h = new ExtendibleHash<>(BUCKET_SIZE);
            for (int i = 0; i < N; i++) {
                h.put(i, i);
            }
            h.saveToFile(tmpFile.toString());
        }

        @TearDown(Level.Iteration)
        public void cleanupFile() throws IOException {
            if (tmpFile != null) {
                Files.deleteIfExists(tmpFile);
            }
        }
    }

    @Benchmark
    public int loadFromDisk10k(PersistedFileState st) throws Exception {
        ExtendibleHash<Integer, Integer> h2 = ExtendibleHash.loadFromFile(st.tmpFile.toString());
        // Читаем все N ключей, чтобы убедиться, что структура восстановилась полностью
        int acc = 0;
        for (int i = 0; i < N; i++) {
            Integer v = h2.get(i);
            acc += (v == null ? 0 : v);
        }
        return acc;
    }

    /* ------------------------- End-to-end pipeline ------------------------ */

    @Benchmark
    public int insertSaveLoad10k() throws Exception {
        ExtendibleHash<Integer, Integer> h = new ExtendibleHash<>(BUCKET_SIZE);
        for (int i = 0; i < N; i++) {
            h.put(i, i);
        }
        Path tmp = Files.createTempFile("exthash_bench_pipeline_", ".dat");
        try {
            h.saveToFile(tmp.toString());
            ExtendibleHash<Integer, Integer> h2 = ExtendibleHash.loadFromFile(tmp.toString());

            // Проверим несколько значений (каждую 1000-ю запись), чтобы не раздувать время теста.
            int acc = 0;
            for (int i = 0; i < N; i += 1000) {
                Integer v = h2.get(i);
                acc += (v == null ? 0 : v);
            }
            return acc;
        } finally {
            Files.deleteIfExists(tmp);
        }
    }
}
