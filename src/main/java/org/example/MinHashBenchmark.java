package org.example;

import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Два бенча:
 * 1) testBuildSignatures — построение сигнатур для corpora (имитирует "построение индекса").
 * 2) testEstimateSimilarity — оценка похожести по двум заранее построенным сигнатурам.
 *
 * Параметры подобраны, чтобы работать быстро и стабильно.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class MinHashBenchmark {

    // -------- параметры нагрузки (можешь варьировать при желании) --------
    @Param({"64"})         // длина сигнатуры
    private int numHash;

    @Param({"100"})        // кол-во документов
    private int numDocs;

    @Param({"800"})        // сколько шинглов на документ
    private int setSize;

    @Param({"50000"})      // размер "вселенной" шинглов (id в [0..universe))
    private int universe;

    private List<Set<Integer>> docs;     // набор документов как множеств шинглов
    private MinHash minhash;             // MinHash с фиксированными функциями
    private List<int[]> signatures;      // заранее построенные сигнатуры (для теста похожести)
    private int idxA, idxB;              // случайная пара документов для сравнения
    private final long seed = 42L;       // сид для воспроизводимости

    @Setup(Level.Trial)
    public void setup() {
        // 1) генерируем корпус документов — случайные множества целых
        Random rnd = new Random(123);
        docs = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            HashSet<Integer> s = new HashSet<>(setSize * 2);
            while (s.size() < setSize) {
                s.add(rnd.nextInt(universe));
            }
            docs.add(s);
        }

        // 2) создаём MinHash и сразу считаем сигнатуры (для бенча похожести)
        minhash = new MinHash(numHash, seed);
        signatures = new ArrayList<>(numDocs);
        for (Set<Integer> doc : docs) {
            signatures.add(minhash.signatureFor(doc));
        }

        // 3) выбираем две разные сигнатуры для сравнения
        Random pairRnd = new Random(777);
        idxA = pairRnd.nextInt(numDocs);
        do {
            idxB = pairRnd.nextInt(numDocs);
        } while (idxB == idxA);
    }

    @Benchmark
    public void testBuildSignatures() {
        // Имитируем построение индекса: считаем сигнатуры для всего корпуса.
        MinHash local = new MinHash(numHash, seed);
        for (Set<Integer> doc : docs) {
            local.signatureFor(doc);
        }
    }

    @Benchmark
    public void testEstimateSimilarity() {
        // Просто считаем оценку жаккарда по двум готовым сигнатурам.
        int[] a = signatures.get(idxA);
        int[] b = signatures.get(idxB);
        MinHash.estimateJaccard(a, b);
    }
}
