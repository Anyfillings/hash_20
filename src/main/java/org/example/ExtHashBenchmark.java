package org.example;

import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ExtHashBenchmark {
    private static final int keynum = 10000;
    private ArrayList<String> inputData;
    private ExtendibleHash<String, String> eh;
    private final int bucketSize = 10;
    private String key;

    @Setup(Level.Trial)
    public void setUp() {
        inputData = new ArrayList<>();
        for (int i = 0; i < keynum; i++) {
            inputData.add("key" + i);
        }

        eh = new ExtendibleHash<>(bucketSize);
        for (String k : inputData) {
            eh.put(k, k);
        }

        Random random = new Random();
        key = "key" + random.nextInt(keynum); // случайный ключ для поиска
    }

    @Benchmark
    public void testBuildFullTable() {
        ExtendibleHash<String, String> local = new ExtendibleHash<>(bucketSize);
        for (String k : inputData) {
            local.put(k, k);
        }
    }

    @Benchmark
    public void testLookupKey() {
        eh.get(this.key);
    }
}
