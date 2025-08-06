package org.example;

import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PerfectHashBenchmark {
    private static final int keynum = 500; // уменьшенный объём
    private List<String> keys;
    private PerfectHash<String> ph;
    private String testKey;

    @Setup(Level.Trial)
    public void setUp() {
        keys = new ArrayList<>();
        for (int i = 0; i < keynum; i++) {
            keys.add("key" + i);
        }
        ph = new PerfectHash<>(keys);
        testKey = keys.get(new Random().nextInt(keynum));
    }

    @Benchmark
    public void testBuildPerfectHashTable() {
        new PerfectHash<>(keys);
    }

    @Benchmark
    public void testLookupKey() {
        ph.contains(testKey);
    }
}
