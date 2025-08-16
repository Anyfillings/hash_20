package org.example;

import org.openjdk.jmh.annotations.*;
import java.util.concurrent.TimeUnit;

/**
 * Бенчмарк вставки/чтения в extendible hash.
 * Главное изменение: размер тестовых данных = 10_000.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class ExtHashBenchmark {

    private static final int N = 10_000; // <-- требование: 10000
    private static final int BUCKET_SIZE = 4;

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
}
