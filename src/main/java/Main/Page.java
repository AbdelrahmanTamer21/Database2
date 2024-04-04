package Main;

import java.io.*;
import java.util.*;
import Exception.DBAppException;
import Utilities.Serializer;

public class Page implements Serializable {
    private final String tableName;
    private int serial;
    private Vector<Tuple> tuples;
    private final int maxSize = DBApp.pageSize;

    public Page(Vector<Tuple> tuples, String tableName, int serial) {
        this.tuples = tuples;
        this.tableName = tableName;
        this.serial = serial;
        Serializer.serializePage(this,tableName, serial);
    }

    public Vector<Tuple> getTuples(){
        return tuples;
    }

    // Method to add tuple to vector with checks
    public void addTuple(Tuple tuple) throws DBAppException {
        if (tuples.size() < maxSize) {
            if(!isDuplicate(tuple.getPrimaryKeyValue())){ //check if the primary key is duplicated
                tuples.add(tuple);
                sort();
            } else {
                throw new DBAppException("Primary key already exists");
            }
        } else {
            // Main.Page is full, cannot add more tuples
            System.out.println("Page is full");
        }
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

    // Method to check if the page is empty
    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    // Method to check if the page is full
    public boolean isFull(){
        return tuples.size()==maxSize;
    }

    // Sort the tuples in the page based on the first value in the tuple's string array
    public void sort(){
        //e3tebarn en awl haga heya el key :)
        tuples.sort(Comparator.comparing(s -> (int) s.getPrimaryKeyValue()));
    }

    // Method to insert a new tuple in the page with checks over the types of the values
    public void insert(Hashtable<String,Object> values, LinkedHashMap<String,String> attributes, String primaryKey) throws DBAppException {
        //iterate over the attributes and check if the value is of the same type
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String type = entry.getValue();
            try {
                Class<?> cls = Class.forName(type);
                if(!cls.isInstance(values.get(key))) {
                    throw new DBAppException("Wrong type");
                }
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }
        }
        this.addTuple(new Tuple(values,primaryKey));
    }

    public Hashtable<String, Object> update(Object primaryKey, Hashtable<String,Object> values, LinkedHashMap<String,String> attributes) throws DBAppException {
        int indexToUpdate = binarySearchString(primaryKey);
        int i = 0;
        //iterate over the attributes and check if the value is of the same type
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String type = entry.getValue();
            try {
                Class<?> cls = Class.forName(type);
                if(!cls.isInstance(values.get(key))) {
                    throw new DBAppException("Wrong type");
                }
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }
        }
        Tuple tuple = tuples.get(indexToUpdate);
        tuples.get(indexToUpdate).setValues(values);
        return tuples.get(indexToUpdate).getValues();
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

    // Method to check if this value already exists in the page
    public boolean isDuplicate(Object primaryKey){
        return binarySearchString(primaryKey) != -1;
    }

    public int getSerial() {
        return serial;
    }

    public void setSerial(int serial) {
        this.serial = serial;
    }
}
