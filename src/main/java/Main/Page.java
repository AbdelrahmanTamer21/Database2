package Main;

import Exception.DBAppException;

import java.io.Serializable;
import java.util.*;

public class Page implements Serializable {
    private final int serial;
    private final Vector<Tuple> tuples;
    private final int maxSize = DBApp.pageSize;

    public Page(Vector<Tuple> tuples, int serial) {
        this.tuples = tuples;
        this.serial = serial;
    }

    public Vector<Tuple> getTuples(){
        return tuples;
    }

    // Method to add tuple to vector with checks
    public void addTuple(Tuple tuple) throws DBAppException {
        if (tuples.size() >= maxSize) {
            throw new DBAppException("Page is full");
        }
        if (isDuplicate(tuple.getPrimaryKeyValue())) {
            throw new DBAppException("Primary key already exists");
        }
        tuples.add(findIndexToInsert(tuple.getPrimaryKeyValue()), tuple);
        //sort();
    }

    // Method to remove last tuple from vector and return it for shifting
    public Tuple removeLastTuple(){
        return tuples.remove(tuples.size()-1);
    }

    public Tuple getLastTuple(){
        return tuples.get(tuples.size()-1);
    }

    public String toString(){
        StringBuilder result = new StringBuilder();
        for (Tuple tuple : tuples) {
            result.append(tuple.toString()).append(",");
        }
        return result.toString();
    }

    // Method to check if the page is full
    public boolean isFull(){
        return tuples.size()==maxSize;
    }

    public int getSize(){
        return tuples.size();
    }

    // Method to insert a new tuple in the page with checks over the types of the values
    public void insert(Hashtable<String,Object> values, LinkedHashMap<String,String> attributes, String primaryKey) throws DBAppException {
        //iterate over the attributes and check if the value is of the same type
        LinkedHashMap<String, Object> data = new LinkedHashMap<>();
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            Object value = values.getOrDefault(key, null);
            if (value != null && !entry.getValue().equals(value.getClass().getName())) {
                throw new DBAppException("Tuple's data type doesn't match the column's data type");
            }
            data.put(key, value);
        }
        this.addTuple(new Tuple(data,primaryKey));
    }

    public HashMap<String, Object> update(Object primaryKeyVal, String primaryKey, Hashtable<String,Object> values, LinkedHashMap<String,String> attributes) throws DBAppException {
        int indexToUpdate = binarySearchString(primaryKeyVal);
        if (indexToUpdate == -1) {
            throw new DBAppException("Primary key not found");
        }
        Tuple tuple = tuples.get(indexToUpdate);
        HashMap<String,Object> data = new HashMap<>(tuple.getValues());
        //iterate over the attributes and check if the value is of the same type
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            if (!Objects.equals(key, primaryKey) && values.containsKey(key)) {
                Object value = values.get(key);
                if (value != null && !entry.getValue().equals(value.getClass().getName())) {
                    throw new DBAppException("Tuple's data type doesn't match the column's data type");
                }
                tuple.getValues().put(key, value);
            }
        }
        return data;
    }

    public Tuple delete(Object primaryKey) throws DBAppException {
        int index = binarySearchString(primaryKey);
        if(index == -1){
            throw new DBAppException("Primary key not found");
        }
        return tuples.remove(index);
    }

    // Method to search for a primary Key in the page using binary search
    public int binarySearchString(Object key){
        int low = 0;
        int high = tuples.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Object midValue = tuples.get(mid).getPrimaryKeyValue();
            int cmp = switch (key.getClass().getSimpleName()) {
                case "Integer" -> Integer.compare((int) key, (int) midValue);
                case "String" -> ((String) key).compareTo((String) midValue);
                case "Double" -> Double.compare((double) key, (double) midValue);
                default -> throw new IllegalStateException("Unexpected value: " + key.getClass().getSimpleName());
            };

            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                return mid; // key found
            }
        }

        return -1;
    }

    public int findIndexToInsert(Object key){
        int low = 0;
        int high = tuples.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Object midValue = tuples.get(mid).getPrimaryKeyValue();
            int cmp = switch (key.getClass().getSimpleName()) {
                case "Integer" -> Integer.compare((int) key, (int) midValue);
                case "String" -> ((String) key).compareTo((String) midValue);
                case "Double" -> Double.compare((double) key, (double) midValue);
                default -> throw new IllegalStateException("Unexpected value: " + key.getClass().getSimpleName());
            };
            if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        return low;
    }

    // Method to check if this value already exists in the page
    public boolean isDuplicate(Object primaryKey){
        return binarySearchString(primaryKey) != -1;
    }

    public int getSerial() {
        return serial;
    }

    public Object[] getMinMax(String key){
        Object[] minMax = new Object[2];
        minMax[0] = tuples.get(0).getValues().get(key);
        minMax[1] = tuples.get(tuples.size()-1).getValues().get(key);
        return minMax;
    }

}
