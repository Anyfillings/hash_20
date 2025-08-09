package org.example;

import java.util.*;

/**
 * Простейшая реализация MinHash для оценки жаккардовой похожести множеств.
 * Работает с множествами целых элементов (например, шинглов), но есть
 * удобные помощники для строк и шинглинга.
 */
public class MinHash {
    private final int numHash;     // количество хеш-функций (длина сигнатуры)
    private final int[] a;         // параметры хешей: a_i
    private final int[] b;         // параметры хешей: b_i
    private final int prime;       // простое число для модульной арифметики

    /**
     * Конструктор. Создаёт семейство хеш-функций вида h_i(x) = (a_i * x + b_i) mod P.
     * @param numHash количество хеш-функций (рекомендация: 64, 128, 256)
     * @param seed    сид генератора для воспроизводимости
     */
    public MinHash(int numHash, long seed) {
        if (numHash <= 0) throw new IllegalArgumentException("numHash must be > 0");
        this.numHash = numHash;
        this.a = new int[numHash];
        this.b = new int[numHash];

        // Большое простое (влезает в int): 2_147_483_629 — следующее после Integer.MAX_VALUE? Нет.
        // Возьмём стандартное большое простое < 2^31: 2_147_483_647 (Мерсенна, тоже простое).
        this.prime = 2_147_483_647;

        Random rnd = new Random(seed);
        for (int i = 0; i < numHash; i++) {
            // a_i ∈ [1, P-1], b_i ∈ [0, P-1]
            int ai = 0;
            while (ai == 0) ai = 1 + rnd.nextInt(prime - 1);
            int bi = rnd.nextInt(prime);
            a[i] = ai;
            b[i] = bi;
        }
    }

    public int numHash() {
        return numHash;
    }

    /**
     * Строит сигнатуру MinHash для множества целых элементов.
     * @param set множество элементов (например, хэши шинглов)
     * @return массив длины numHash с минимумами по каждой хеш-функции
     */
    public int[] signatureFor(Set<Integer> set) {
        int[] sig = new int[numHash];
        Arrays.fill(sig, Integer.MAX_VALUE);

        for (int x : set) {
            int xi = normalize(x);
            for (int i = 0; i < numHash; i++) {
                // h_i(x) = (a_i * x + b_i) mod P
                long val = ( (long)a[i] * xi + (long)b[i] ) % prime;
                int hv = (int) val;
                if (hv < sig[i]) sig[i] = hv;
            }
        }
        return sig;
    }

    /**
     * Оценка жаккардова сходства по двум сигнатурам.
     * @return доля позиций, где элементы сигнатур совпали.
     */
    public static double estimateJaccard(int[] s1, int[] s2) {
        if (s1.length != s2.length) throw new IllegalArgumentException("Signatures length mismatch");
        int eq = 0;
        for (int i = 0; i < s1.length; i++) {
            if (s1[i] == s2[i]) eq++;
        }
        return (double) eq / s1.length;
    }

    // ===================== Утилиты для строк/шинглов =====================

    /**
     * Хэш строки в стабильный неотрицательный int (для превращения токенов/шинглов в числа).
     */
    public static int hashString(String s) {
        // Берём hashCode, делаем неотрицательным и чуть перемешаем Objects.hash
        int h = s.hashCode();
        h ^= (h >>> 16);
        return h & 0x7fffffff;
    }

    /**
     * Превращает набор строковых токенов в множество целых (для MinHash).
     */
    public static Set<Integer> toIntSet(Collection<String> tokens) {
        HashSet<Integer> set = new HashSet<>(Math.max(16, tokens.size() * 2));
        for (String t : tokens) set.add(hashString(t));
        return set;
    }

    /**
     * Простейший шинглинг: N-граммы символов.
     */
    public static Set<Integer> shingles(String text, int k) {
        if (k <= 0) throw new IllegalArgumentException("k must be > 0");
        HashSet<Integer> set = new HashSet<>();
        if (text == null) return set;
        String s = text.length() < k ? text : text;
        for (int i = 0; i + k <= s.length(); i++) {
            String sh = s.substring(i, i + k);
            set.add(hashString(sh));
        }
        return set;
    }

    private static int normalize(int x) {
        // Приведём к диапазону [0, prime)
        int y = x % 0x7fffffff;
        if (y < 0) y += 0x7fffffff;
        return y;
    }
}
