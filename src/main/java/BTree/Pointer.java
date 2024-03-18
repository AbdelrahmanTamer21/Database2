package BTree;

public class Pointer<TKey extends Comparable<TKey>, TValue>{
    private TKey key;
    private TValue value;

    public Pointer(TKey key, TValue value) {
        this.key = key;
        this.value = value;
    }

    public TKey getKey() {
        return key;
    }

    public TValue getValue() {
        return value;
    }

    @Override
    public String toString() {
        return key + ", " + value;
    }
}
