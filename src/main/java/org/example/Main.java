package org.example;

public class Main {
    public static void main(String[] args) {
        ExtendibleHash<Integer, String> hash = new ExtendibleHash<>(2);

        hash.put(1, "one");
        hash.put(2, "two");
        hash.put(3, "three");
        hash.put(4, "four");

        hash.printStatus();

        System.out.println("Get 2: " + hash.get(2));
        hash.remove(2);
        System.out.println("Get 2 after removal: " + hash.get(2));
    }
}
