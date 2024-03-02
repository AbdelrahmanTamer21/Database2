import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
    private static final long serialVersionUID = 1L;
    private Vector<Tuple> tuples;
    private transient int maxSize;

    public Page(Vector<Tuple> tuples) {
        this.tuples = tuples;
    }

    public void addTuple(Tuple tuple) {
        if (tuples.size() < maxSize) {
            tuples.add(tuple);
        } else {
            // Page is full, cannot add more tuples
            System.out.println("Page is full, cannot add more tuples.");
        }
    }

    public String toString(){
        StringBuilder result = new StringBuilder();
        for (Tuple tuple : tuples) {
            result.append(tuple.toString()).append(",");
        }
        return result.toString();
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    public boolean isFull(){
        return tuples.size()==maxSize;
    }

    public int binarySearchString(String[] arr, String x){
        int low = 0;
        int high = arr.length - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            int comparisonResult = x.compareTo(arr[mid]);

            if (comparisonResult == 0) {
                return mid; // String found at index mid
            } else if (comparisonResult > 0) {
                low = mid + 1; // Search in the right half
            } else {
                high = mid - 1; // Search in the left half
            }
        }

        return -1;
    }
}
