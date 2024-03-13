package Main;

import java.io.*;
import java.util.*;
import Exception.DBAppException;

public class Page implements Serializable {
    private final String tableName;
    private int serial;
    private Vector<Tuple> tuples;
    private final int maxSize = DBApp.pageSize;

    public Page(Vector<Tuple> tuples, String tableName, int serial) {
        this.tuples = tuples;
        this.tableName = tableName;
        this.serial = serial;
        serialize(this,tableName, serial);
    }

    public Vector<Tuple> getTuples(){
        return tuples;
    }

    // Method to add tuple to vector with checks
    public void addTuple(Tuple tuple) throws DBAppException {
        if (tuples.size() < maxSize) {
            if(!isDuplicate(tuple.getValues()[0])) { //check if the primary key is duplicated
                tuples.add(tuple);
                sort();
            } else {
                throw new DBAppException("Primary key is duplicated");
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
        return tuples.get(tuples.size()-1);}

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
        //e3tebarn en awl haga heya el key:)
        tuples.sort(Comparator.comparing(s -> s.getValues()[0]));
    }

    // Method to insert a new tuple in the page with checks over the types of the values
    public void insert(Hashtable<String,Object> values, LinkedHashMap<String,String> attributes) throws DBAppException {
        String[] dataValues = new String[attributes.size()];
        int i = 0;
        //iterate over the attributes and check if the value is of the same type
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String type = entry.getValue();
            try {
                Class<?> cls = Class.forName(type);
                if(cls.isInstance(values.get(key))) {
                    String data = String.valueOf(values.get(key));
                    dataValues[i] = data;
                    i++;
                } else {
                    throw new DBAppException("Wrong type");
                }
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }
        }
        this.addTuple(new Tuple(dataValues));
    }

    public void update(String primaryKey, Hashtable<String,Object> values) throws DBAppException {
        int indexToUpdate = binarySearchString(primaryKey);
        LinkedHashMap<String,String> attributes = Table.getAttributes(tableName);
        String[] dataValues = new String[attributes.size()];
        int i = 0;
        //iterate over the attributes and check if the value is of the same type
        for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String key = entry.getKey();
            String type = entry.getValue();
            try {
                Class<?> cls = Class.forName(type);
                if(cls.isInstance(values.get(key))) {
                    String data = String.valueOf(values.get(key));
                    dataValues[i] = data;
                    i++;
                } else {
                    throw new DBAppException("Wrong type");
                }
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }
        }
        tuples.get(indexToUpdate).setValues(dataValues);
    }

    public void delete(String primaryKey) throws DBAppException {
        int index = binarySearchString(primaryKey);
        if(index == -1){
            throw new DBAppException("Primary key not found");
        }
        tuples.remove(index);
    }

    // Method to search for a primary Key in the page using binary search
    public int binarySearchString(String key){
        int low = 0;
        int high = tuples.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            String midValue = tuples.get(mid).getValues()[0];
            int cmp = key.compareTo(midValue);

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

    // Method to serialize the page
    public static void serialize(Page page,String tableName,int serial) {
        try {
            //you may also write this verbosely as
            // FileOutputStream fileOutputStream = new FileOutputStream(fileName);
            FileOutputStream fileOutputStream = new FileOutputStream("Pages/" + tableName + "/" + tableName + serial + ".ser");

            ObjectOutputStream objOutputStream = new ObjectOutputStream(fileOutputStream);

            objOutputStream.writeObject(page);
            //we don't want a memory leak if we can avoid it
            fileOutputStream.close();
            objOutputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to deserialize the page
    public static Page deserialize(String tableName,int serial){
        try {
            FileInputStream fileInputStream = new FileInputStream ("Pages/" + tableName + "/" + tableName + serial +".ser");

            ObjectInputStream  objInputStream = new ObjectInputStream (fileInputStream);

            Page page = (Page) objInputStream.readObject();

            objInputStream.close();
            fileInputStream.close();

            return page;

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // Method to check if this value already exists in the page
    public boolean isDuplicate(String primaryKey){
        return binarySearchString(primaryKey) != -1;
    }

    public int getSerial() {
        return serial;
    }

    public void setSerial(int serial) {
        this.serial = serial;
    }
}
