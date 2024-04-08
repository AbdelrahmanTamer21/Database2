package BTree;

public record Pointer<TKey extends Comparable<TKey>, TValue>(TKey key, TValue value) {

    @Override
    public String toString() {
        return "(" + key + ", " + value + ")";
    }
}
