package org.example;

import java.util.*;

public class PerfectHash<K> {
    private Object[] table;
    private int m;
    private int seed;

    public PerfectHash(List<K> keys) {
        int n = keys.size();
        m = n * n;
        Random rand = new Random();
        boolean success = false;

        while (!success) {
            seed = rand.nextInt();
            success = true;
            table = new Object[m];
            for (K key : keys) {
                int idx = Math.abs(Objects.hash(key, seed)) % m;
                if (table[idx] != null) {
                    success = false;
                    break;
                }
                table[idx] = key;
            }
        }
    }

    public boolean contains(K key) {
        int idx = Math.abs(Objects.hash(key, seed)) % m;
        return key.equals(table[idx]);
    }
}
