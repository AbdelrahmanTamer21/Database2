package Main;

import BTree.BTree;
import BTree.Pointer;
import Exception.DBAppException;
import Utilities.Serializer;

import java.io.File;
import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    private final String tableName;
    private final String primaryKey;
    private Vector<String> pageNames;
    private final LinkedHashMap<String,String> attributes;
    private int size;
    private Vector<String> bTrees;
    private Vector<String> indexNames;
    private Vector<BTree<?,String>> indices;

    public Table(String name, String primaryKeyColumn, LinkedHashMap<String, String> attributes) {
        this.tableName = name;
        this.primaryKey = primaryKeyColumn;
        this.pageNames = new Vector<>();
        this.attributes = attributes;
        this.size = 0;
        bTrees = new Vector<>();
        indexNames = new Vector<>();
        indices = new Vector<BTree<?, String>>();
        createDirectories();
    }

    /**
     * Helper Methods
     */
    public boolean isEmpty(){
        return pageNames.size() == 0;
    }
    public int getSize(){
        return size;
    }
    private void createDirectories() {
        File pagesDir = new File("Pages/" + tableName);
        pagesDir.mkdirs();
        File indexDir = new File("Indices/" + tableName);
        indexDir.mkdirs();
    }
    // Method to get all the actual pages of a table
    private List<Page> getPages(String tableName){
        List<Page> pages = new ArrayList<>();
        pageNames.sort(String::compareTo);
        for (String pageName : pageNames) {
            pages.add(Serializer.deserializePage(tableName,Integer.parseInt( pageName.substring(tableName.length(),pageName.length()-4) ) ) );
        }
        return pages;
    }
    public List<BTree<?,String>> getBTrees(){
        List<BTree<?,String>> bTreesList = new ArrayList<>();
        for(String btree: bTrees){
            String colName = btree.replace("Index.ser","");
            bTreesList.add(BTree.deserialize(tableName,colName));
        }
        return bTreesList;
    }
    public <TKey extends Comparable<TKey>> BTree<TKey, String> getBTree(String colName) {
        if(bTrees.contains(colName+"Index.ser")){
            switch (attributes.get(colName)){
                case "java.lang.String" -> {
                    return (BTree<TKey, String>) BTree.deserialize(tableName,colName);
                }
                case "java.lang.Integer" -> {
                    return (BTree<TKey, String>) BTree.deserialize(tableName,colName);
                }
                case "java.lang.Double" -> {
                    return (BTree<TKey, String>) BTree.deserialize(tableName,colName);
                }
            }
        }
        return null;
    }
    public Page getPageAtPosition(int position){
        pageNames.sort(String::compareTo);
        String pageName = pageNames.get(position);
        return Serializer.deserializePage(tableName,Integer.parseInt( pageName.substring(tableName.length(),pageName.length()-4) ) );
    }

    private static boolean doesIndexExist(String tableName, String colName){
        File folder = new File("Indices/"+tableName);
        folder.mkdirs();
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < Objects.requireNonNull(listOfFiles).length; i++) {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith("Index.ser") && listOfFiles[i].getName().startsWith(colName)) {
                return true;
            }
        }
        return false;
    }
    private void updateIndices(String strTableName) throws DBAppException {
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                BTree bTree1 = BTree.deserialize(strTableName,colName);

                createIndex(colName, bTree1.getIndexName(),true);
            }
        }
    }
    public Vector<String> getPageNames() {
        return pageNames;
    }
    public Vector<String> getIndices(){
        return bTrees;
    }

    public Vector<BTree<?,String>> getBTreeIndicies(){
        return indices;
    }

    public LinkedHashMap<String, String> getAttributes() {
        return attributes;
    }

    private boolean checkColumnsExist(Hashtable<String,Object> htblColNameValue){
        for (String entry: htblColNameValue.keySet()){
            if(!attributes.containsKey(entry)){
                return false;
            }
        }
        return true;
    }


    /**
     * Main methods
     */
    // Method to insert a new tuple in the table
    public void insertTuple(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        if(!htblColNameValue.containsKey(primaryKey)){
            throw new DBAppException("Primary key is not found");
        }
        if(!checkColumnsExist(htblColNameValue)){
            throw new DBAppException("Tuple contains columns that aren't in the table");
        }
        List<Page> pages = getPages(tableName);
        if(pageNames.isEmpty()){
            // Create a new page and insert the new tuple in it
            Page page = new Page(new Vector<>(),1);
            page.insert(htblColNameValue, attributes, primaryKey);
            pageNames.add(tableName + "1.ser");
            Serializer.serializePage(page,tableName,1);
        }else {
            // Figure out which page to insert the new tuple in
            int serialToInsertIn;
            if(doesIndexExist(tableName,primaryKey)){
                long startTime1 = System.nanoTime();
                getBTree(primaryKey);
                long endTime1 = System.nanoTime();
                System.out.println((endTime1-startTime1)/1e6);
                switch (attributes.get(primaryKey)){
                    case "java.lang.String" -> ((BTree<String,String>)getBTree(primaryKey)).getPageNumberForInsert((String) htblColNameValue.get(primaryKey));
                    case "java.lang.Integer" -> ((BTree<Integer,String>)getBTree(primaryKey)).getPageNumberForInsert((Integer) htblColNameValue.get(primaryKey));
                    case "java.lang.Double" -> ((BTree<Double,String>)getBTree(primaryKey)).getPageNumberForInsert((Double) htblColNameValue.get(primaryKey));
                    default -> throw new IllegalStateException("Unexpected value: " + attributes.get(primaryKey));
                };
                serialToInsertIn = switch (attributes.get(primaryKey)){
                    case "java.lang.String" -> ((BTree<String,String>)getBTree(primaryKey)).getPageNumberForInsert((String) htblColNameValue.get(primaryKey));
                    case "java.lang.Integer" -> ((BTree<Integer,String>)getBTree(primaryKey)).getPageNumberForInsert((Integer) htblColNameValue.get(primaryKey));
                    case "java.lang.Double" -> ((BTree<Double,String>)getBTree(primaryKey)).getPageNumberForInsert((Double) htblColNameValue.get(primaryKey));
                    default -> throw new IllegalStateException("Unexpected value: " + attributes.get(primaryKey));
                };
            }else {
                long startTime1 = System.nanoTime();
                serialToInsertIn = findPageToInsert(pages,htblColNameValue.get(primaryKey));
                long endTime1 = System.nanoTime();
                System.out.println((endTime1-startTime1)/1e6);
            }
            // If the primary key already exists it returns -1, throw an exception
            if(serialToInsertIn == -1){
                throw new DBAppException("Primary key already exists");
            }
            // If the page is full, shift values to other pages, else insert the new tuple in the page
            if(pages.get(serialToInsertIn-1).isFull()){
                shiftValuesToOtherPages(pages,serialToInsertIn,tableName,htblColNameValue);
                updateIndices(tableName);
                return;
            }else{
                pages.get(serialToInsertIn-1).insert(htblColNameValue,attributes, primaryKey);
                Serializer.serializePage(pages.get(serialToInsertIn-1),tableName,serialToInsertIn);
            }
        }
        size++;
        int serialToInsertIn = findPageForCertainValue(pages,htblColNameValue.get(primaryKey));
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert((String) htblColNameValue.get(colName),serialToInsertIn + "-" + htblColNameValue.get(primaryKey));
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert((int) htblColNameValue.get(colName),serialToInsertIn + "-" + htblColNameValue.get(primaryKey));
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert((double) htblColNameValue.get(colName),serialToInsertIn + "-" + htblColNameValue.get(primaryKey));
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    // Method to update a certain tuple
    public void updateTuple(String primaryKey,Hashtable<String,Object> values) throws DBAppException {
        if(values.containsKey(this.primaryKey)){
            throw new DBAppException("The input row wants to change the primary key");
        }
        if(values.size()>attributes.size()-1){
            throw new DBAppException("The Tuple has more columns than the table's columns");
        }
        List<Page> pages = getPages(tableName);
        int pageToUpdateIn;
        HashMap<String, Object> data;
        switch (attributes.get(this.primaryKey)){
            case "java.lang.String" -> {
                pageToUpdateIn = findPageForCertainValue(pages, primaryKey);
                data = pages.get(pageToUpdateIn-1).update(primaryKey, this.primaryKey,values, attributes);
            }
            case "java.lang.Integer" -> {
                pageToUpdateIn = findPageForCertainValue(pages, Integer.parseInt(primaryKey));
                data = pages.get(pageToUpdateIn-1).update(Integer.parseInt(primaryKey), this.primaryKey, values, attributes);
            }
            case "java.lang.Double" -> {
                pageToUpdateIn = findPageForCertainValue(pages, Double.parseDouble(primaryKey));
                data = pages.get(pageToUpdateIn-1).update(Double.parseDouble(primaryKey), this.primaryKey, values, attributes);
            }
            default -> throw new IllegalStateException("Unexpected value: " + attributes.get(this.primaryKey));
        }
        Serializer.serializePage(pages.get(pageToUpdateIn-1),tableName,pages.get(pageToUpdateIn-1).getSerial());

        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);
                if(!values.containsKey(colName)){
                    return;
                }

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((String)data.get(colName),pageToUpdateIn + "-" + primaryKey);
                        bTree1.insert((String) values.get(colName),pageToUpdateIn + "-" + primaryKey);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((int) data.get(colName),pageToUpdateIn + "-" + primaryKey);
                        bTree1.insert((int) values.get(colName),pageToUpdateIn + "-" + primaryKey);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((double) data.get(colName),pageToUpdateIn + "-" + primaryKey);
                        bTree1.insert((double) values.get(colName),pageToUpdateIn + "-" + primaryKey);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    // Method to delete tuples
    public void deleteTuples(Hashtable<String, Object> values) throws DBAppException {
        for(Map.Entry<String, Object> entry: values.entrySet()){
            if(!attributes.containsKey(entry.getKey())){
                throw new DBAppException("The Tuple contains come columns that aren't in the table");
            }
            if(!attributes.get(entry.getKey()).equals(entry.getValue().getClass().getName())){
                throw new DBAppException("Tuple's data type doesn't match the column's data type");
            }
        }
        ArrayList<Object> results = new ArrayList<>();
        List<Page> pages = getPages(tableName);
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            ArrayList<Object> satisfyingTuples = findTuplesSatisfyingCondition(entry, pages);
            if (satisfyingTuples.isEmpty()) {
                return;
            }
            if (results.isEmpty()) {
                results = satisfyingTuples;
            } else {
                results = intersect(results, satisfyingTuples);
            }
        }
        for (Object result : results) {
            deleteTuple(result);
        }
    }

    private ArrayList<Object> findTuplesSatisfyingCondition(Map.Entry<String,Object> entry, List<Page> pages) {
        ArrayList<Object> satisfyingTuples = new ArrayList<>();
        if(doesIndexExist(tableName,entry.getKey())){
            switch (attributes.get(entry.getKey())) {
                case "java.lang.String" -> {
                    BTree<String, String> bTree = BTree.deserialize(tableName, entry.getKey());
                    LinkedList<Pointer<String, String>> pointers = bTree.getEqualKeys((String) entry.getValue());
                    for (Pointer<String, String> pointer : pointers) {
                        Page page = pages.get(Integer.parseInt(pointer.getValue().split("-")[0])-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(Integer.parseInt(pointer.getValue().split("-")[1])));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
                case "java.lang.Integer" -> {
                    BTree<Integer, String> bTree = BTree.deserialize(tableName, entry.getKey());
                    LinkedList<Pointer<Integer, String>> pointers = bTree.getEqualKeys((Integer) entry.getValue());
                    for (Pointer<Integer, String> pointer : pointers) {
                        Page page = pages.get(Integer.parseInt(pointer.getValue().split("-")[0])-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(Integer.parseInt(pointer.getValue().split("-")[1])));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
                case "java.lang.Double" -> {
                    BTree<Double, String> bTree = BTree.deserialize(tableName, entry.getKey());
                    LinkedList<Pointer<Double, String>> pointers = bTree.getEqualKeys((Double) entry.getValue());
                    for (Pointer<Double, String> pointer : pointers) {
                        Page page = pages.get(Integer.parseInt(pointer.getValue().split("-")[0])-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(Integer.parseInt(pointer.getValue().split("-")[1])));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
            }
        }else {
            for (Page page : pages) {
                for (Tuple tuple : page.getTuples()) {
                    if (tuple.getValues().get(entry.getKey()).equals(entry.getValue())) {
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
            }
        }
        return satisfyingTuples;
    }

    // Method to find the intersection between two lists
    private ArrayList<Object> intersect(ArrayList<Object> list1, ArrayList<Object> list2){
        ArrayList<Object> result = new ArrayList<>();
        for (Object o : list1) {
            if(list2.contains(o)){
                result.add(o);
            }
        }
        return result;
    }

    // Method to delete a tuple
    public void deleteTuple(Object primaryKeyVal) throws DBAppException {
        List<Page> pages = getPages(tableName);
        int pageToDeleteFrom = findPageForCertainValue(pages, primaryKeyVal);
        Page page = pages.get(pageToDeleteFrom-1);
        Vector<Tuple> tuples = page.getTuples();
        Tuple tuple = null;
        if(tuples.size() > 1){
            tuple = page.delete(primaryKeyVal);
            Serializer.serializePage(page,tableName,page.getSerial());
        }else{
            int comparisonResult = switch (attributes.get(primaryKey)) {
                case "java.lang.Integer" -> Integer.compare((int) tuples.get(0).getPrimaryKeyValue(), (int) primaryKeyVal);
                case "java.lang.String" -> ((String) tuples.get(0).getPrimaryKeyValue()).compareTo((String) primaryKeyVal);
                case "java.lang.Double" -> Double.compare((double) tuples.get(0).getPrimaryKeyValue(), (double) primaryKeyVal);
                default -> throw new IllegalStateException("Unexpected value: " + tuples.get(0).getPrimaryKeyValue().getClass().getSimpleName());
            };
            if(tuples.size() != 0 && comparisonResult == 0){
                tuple = page.delete(primaryKeyVal);
                List<String> pageNamesList = new LinkedList<>(pageNames);
                pageNamesList = pageNamesList.subList(pageToDeleteFrom-1,pageNamesList.size());
                File file = new File("Pages/" + tableName + "/" + tableName + pages.size()+".ser");
                File folder = new File("Pages/"+tableName);
                File[] listOfFiles = folder.listFiles();
                assert listOfFiles != null;
                listOfFiles[pageToDeleteFrom-1].delete();
                pageNames.remove(pageNames.size()-1);
                if(pages.size() > 1) {
                    for (int i = pageToDeleteFrom - 1; i < pageNamesList.size(); i++) {
                        pages.get(i + 1).setSerial(i + 1);
                        Serializer.serializePage(pages.get(i + 1), tableName, i + 1);
                    }
                }
                file.delete();
            }
        }
        size--;
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((String) tuple.getValues().get(colName),pageToDeleteFrom + "-" + tuple.getPrimaryKeyValue());
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((int) tuple.getValues().get(colName),pageToDeleteFrom + "-" + tuple.getPrimaryKeyValue());
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((double) tuple.getValues().get(colName),pageToDeleteFrom + "-" + tuple.getPrimaryKeyValue());
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    // return the serial of the page to insert the new tuple in
    public int findPageToInsert(List<Page> pages, Object newPrimaryKey){
        int low = 1;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = pages.get(mid);

            if(currentPage.isEmpty()){
                return mid+1;
            }
            // Compare the new string with the first value in the vector of current page
            Tuple tuple = currentPage.getTuples().get(0);
            int comparisonResult = comparePrimaryKey(newPrimaryKey, tuple);

            // If the newPrimaryKey is already in the page, return -1
            if(currentPage.binarySearchString(newPrimaryKey) != -1){
                return -1;
            }
            // If newPrimaryKey is less than or equal to the first value, go left
            if (comparisonResult < 0) {
                return mid;
            }
            // If newString is greater, go right
            else {
                low = mid + 1;
            }
        }
        // If not found, return the last page
        return pages.size();
    }

    public int findPageForCertainValue(List<Page> pages, Object primaryKey){
        int low = 1;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = pages.get(mid);

            if(currentPage.isEmpty()){
                return mid+1;
            }
            // Compare the new string with the first value in the vector of current page
            Tuple tuple = currentPage.getTuples().get(0);
            int comparisonResult = comparePrimaryKey(primaryKey, tuple);

            // If newPrimaryKey is less than or equal to the first value, go left
            if (comparisonResult < 0) {
                return mid;
            }
            // If newString is greater, go right
            else {
                low = mid + 1;
            }
        }
        // If not found, return the last page
        return pages.size()==0?1:pages.size();
    }

    private int comparePrimaryKey(Object primaryKey, Tuple tuple) {
         return switch (attributes.get(this.primaryKey)) {
            case "java.lang.Integer" -> Integer.compare((Integer) primaryKey, (int) tuple.getPrimaryKeyValue());
            case "java.lang.String" -> (String.valueOf(primaryKey).compareTo((String) tuple.getPrimaryKeyValue()));
            case "java.lang.Double" -> Double.compare((Double) primaryKey, (double) tuple.getPrimaryKeyValue());
            default -> throw new IllegalStateException("Unexpected value: " + tuple.getPrimaryKeyValue().getClass().getSimpleName());
        };
    }

    /**
     * Compares the value in a specified column of a tuple with a given value.
     *
     * @param tuple   The tuple containing the value to compare.
     * @param colName The name of the column in the tuple.
     * @param val     The value to compare against.
     * @return A negative value if the value in the column in the tuple is less than the value given,
     *         a positive value if the column in the tuple is greater than the value given,
     *         and 0 if they are both the same.
     */
    private int compareValues(Tuple tuple,String colName,Object val){
        return switch (attributes.get(colName)) {
            case "java.lang.Integer" -> Integer.compare((int) tuple.getValues().get(colName), (int) val);
            case "java.lang.String" -> (String.valueOf(tuple.getValues().get(colName)).compareTo((String) val));
            case "java.lang.Double" -> Double.compare((double) tuple.getValues().get(colName), (double) val);
            default -> throw new IllegalStateException("Unexpected value: " + attributes.get(colName));
        };
    }

    // Method to shift values to other pages if there's no space
    public void shiftValuesToOtherPages(List<Page> pages, int serial, String tableName, Hashtable<String, Object> values) throws DBAppException {
        List<Page> newPages = pages.subList(serial-1, pages.size());

        // If the value to be inserted is larger than the last value in the page create a new page and shift the values from other pages to it
        switch (attributes.get(primaryKey)){
            case "java.lang.String" -> {
                if(String.valueOf(pages.get(serial-1).getLastTuple().getPrimaryKeyValue()).compareTo(String.valueOf(values.get(primaryKey))) < 0){
                    if(serial >= pages.size()){
                        newPages = new LinkedList<>();
                    }else {
                        newPages = pages.subList(serial, pages.size());
                    }
                }
            }
            case "java.lang.Integer" -> {
                if(Integer.parseInt(pages.get(serial-1).getLastTuple().getPrimaryKeyValue().toString()) < Integer.parseInt(String.valueOf(values.get(primaryKey)))){
                    if(serial >= pages.size()){
                        newPages = new LinkedList<>();
                    }else {
                        newPages = pages.subList(serial, pages.size());
                    }
                }
            }
            case "java.lang.Double" -> {
                if(Double.parseDouble(pages.get(serial-1).getLastTuple().getPrimaryKeyValue().toString()) < Double.parseDouble(String.valueOf(values.get(primaryKey)))){
                    if(serial >= pages.size()){
                        newPages = new LinkedList<>();
                    }else {
                        newPages = pages.subList(serial, pages.size());
                    }
                }
            }
        }

        for (Page page : newPages) {
            if (!page.isFull()) {
                page.insert(values,attributes, primaryKey);
                Serializer.serializePage(page,tableName,page.getSerial());
                return;
            }else{
                HashMap<String, Object> lastTupleData = page.removeLastTuple().getValues();
                page.insert(values,attributes, primaryKey);
                Hashtable<String, Object> lastTuple = new Hashtable<>();
                for(Map.Entry<String, String> entry : attributes.entrySet()) {
                    String key = entry.getKey();
                    String type = entry.getValue();
                    switch (type) {
                        case "java.lang.Integer" -> lastTuple.put(key, Integer.parseInt(lastTupleData.get(key).toString()));
                        case "java.lang.String" -> lastTuple.put(key, lastTupleData.get(key).toString());
                        case "java.lang.Double" -> lastTuple.put(key, Double.parseDouble(lastTupleData.get(key).toString()));
                    }
                }
                values = lastTuple;
                Serializer.serializePage(page,tableName,page.getSerial());
            }
        }
        // If no page has space, create a new page and insert the new string
        int newPageId = pages.size() + 1; // Assuming page ids start from 1
        Page newPage = new Page(new Vector<>(), newPageId);
        newPage.insert(values, attributes, primaryKey);
        Serializer.serializePage(newPage,tableName,newPageId);
        pages.add(newPage);
        pageNames.add(tableName + newPageId + ".ser");
    }

    public void createIndex(String colName, String indexName, boolean update) throws DBAppException {
        if(!update && indexNames.contains(indexName)){
            throw new DBAppException("The index was already created on one of the columns");
        }
        if(attributes.get(colName) == null){
            throw new DBAppException("Wrong column name");
        }

        List<Page> pages = getPages(tableName);
        BTree<? ,String> index = new BTree<Comparable, String>(indexName,(!Objects.equals(primaryKey, colName)));
        switch (attributes.get(colName)) {
            case "java.lang.String" -> {
                Vector<Pointer<String, String>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((String) tuple.getValues().get(colName), page.getSerial() + "-" + tuple.getPrimaryKeyValue()));
                    }
                }
                data.sort((o1, o2) -> {
                    String s1 = String.valueOf(o1.getKey());
                    String s2 = String.valueOf(o2.getKey());
                    return s1.compareTo(s2);
                });
                BTree<String, String> bTree = new BTree<String, String>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<String, String> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                index = bTree;
                BTree.serialize(bTree,tableName,colName);
            }
            case "java.lang.Integer" -> {
                Vector<Pointer<Integer, String>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((Integer) tuple.getValues().get(colName), page.getSerial() + "-" + tuple.getPrimaryKeyValue()));
                    }
                }
                data.sort((o1, o2) -> {
                    Integer i1 = Integer.valueOf(o1.getKey());
                    Integer i2 = Integer.valueOf(o2.getKey());
                    return i1.compareTo(i2);
                });
                BTree<Integer, String> bTree = new BTree<Integer, String>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<Integer, String> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                index = bTree;
                BTree.serialize(bTree,tableName,colName);
            }
            case "java.lang.Double" -> {
                Vector<Pointer<Double, String>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((Double) tuple.getValues().get(colName), page.getSerial() + "-" + tuple.getPrimaryKeyValue()));
                    }
                }
                data.sort((o1, o2) -> {
                    Double d1 = o1.getKey();
                    Double d2 = o2.getKey();
                    return d1.compareTo(d2);
                });
                BTree<Double, String> bTree = new BTree<Double, String>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<Double, String> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                index = bTree;
                BTree.serialize(bTree,tableName,colName);
            }
        }
        if(!bTrees.contains(colName+"Index.ser")) {
            bTrees.add(colName+"Index.ser");
            indexNames.add(indexName);
            indices.add(index);
        }

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[]  strarrOperators) throws DBAppException, ClassNotFoundException {
        List<SQLTerm> sqlTerms = new ArrayList<>(Arrays.asList(arrSQLTerms));
        List<String> operations = new ArrayList<>(Arrays.asList(strarrOperators));
        //Primary key value,Whole tuple
        List<HashMap<Object,Tuple>> results = new ArrayList<>();
        if (sqlTerms.isEmpty()) {
            return null;
        }
        if(sqlTerms.size()==1){
           HashMap<Object,Tuple> term = computeSQLTerm(sqlTerms.get(0));
           Collection<Tuple> result = term.values();
           return result.iterator();
        }else if(sqlTerms.size()>1){
            for (SQLTerm sqlTerm : sqlTerms) {
                //sqlterms : name
                //operations: or
                // name , gpa&grade
                // select * from Student where name = “John Noor” or gpa = 1.5 and grade > 'C'
                results.add(computeSQLTerm(sqlTerm));
            }
            while(!operations.isEmpty()){
                int nextIndex = getNextArrayOperation(operations);
                switch (operations.get(nextIndex).toUpperCase()){
                    case "AND" -> {
                        HashMap<Object, Tuple> intersect = getIntersection(results.get(nextIndex), results.get(nextIndex + 1));
                        operations.remove(nextIndex);
                        results.remove(nextIndex);
                        results.remove(nextIndex);
                        results.add(nextIndex,intersect);
                    }
                    case "OR" -> {
                        HashMap<Object, Tuple> union = getUnion(results.get(nextIndex), results.get(nextIndex + 1));
                        operations.remove(nextIndex);
                        results.remove(nextIndex);
                        results.remove(nextIndex);
                        results.add(nextIndex,union);
                    }
                    case "XOR" -> {
                        HashMap<Object, Tuple> xor = getXOR(results.get(nextIndex), results.get(nextIndex + 1));
                        operations.remove(nextIndex);
                        results.remove(nextIndex);
                        results.remove(nextIndex);
                        results.add(nextIndex,xor);
                    }
                }
            }
            Collection<Tuple> result = results.get(0).values();
            return result.iterator();
        }
        return results.get(0).values().iterator();
    }

    public HashMap<Object,Tuple> getIntersection(HashMap<Object, Tuple> hashMap1,HashMap<Object,Tuple> hashMap2){
        HashMap<Object, Tuple> result = new HashMap<>();
        if(hashMap1.size()<hashMap2.size()){
            for(Map.Entry<Object,Tuple> entry : hashMap1.entrySet()){
                if(hashMap2.containsKey(entry.getKey())){
                    result.put(entry.getKey(),entry.getValue());
                }
            }
        }else {
            for(Map.Entry<Object,Tuple> entry : hashMap2.entrySet()){
                if(hashMap1.containsKey(entry.getKey())){
                    result.put(entry.getKey(),entry.getValue());
                }
            }
        }
        return result;
    }

    public HashMap<Object,Tuple> getUnion(HashMap<Object, Tuple> hashMap1,HashMap<Object,Tuple> hashMap2){
        HashMap<Object,Tuple> hashMap = new HashMap<>(hashMap1);
        hashMap.putAll(hashMap2);
        return hashMap;
    }

    public HashMap<Object, Tuple> getXOR(HashMap<Object, Tuple> hashMap1,HashMap<Object,Tuple> hashMap2){
        HashMap<Object, Tuple> xor = getUnion(hashMap1,hashMap2);
        HashMap<Object, Tuple> intersection = getIntersection(hashMap1,hashMap2);
        for(Object key : intersection.keySet()){
            xor.remove(key);
        }
        return xor;
    }

    private int getNextArrayOperation(List<String> operations){
        /*
        int andIndex = -1;
        int orIndex = -1;
        for (int i = 0; i < operations.size(); i++) {
            String operation = operations.get(i);
            switch (operation) {
                case "AND":
                    return i;
                case "OR":
                    if (andIndex == -1) {
                        orIndex = i;
                    }
                    break;
                case "XOR":
                    if (andIndex == -1 && orIndex == -1) {
                        return i;
                    }
                    break;
            }
        }
        return andIndex != -1 ? andIndex : (orIndex != -1 ? orIndex : 0);

         */
        for(int i = 0;i<operations.size();i++){
            if(operations.get(i) == "AND"){
                return i;
            }
        }
        for(int i = 0;i<operations.size();i++){
            if(operations.get(i) == "OR"){
                return i;
            }
        }
        for(int i = 0;i<operations.size();i++){
            if(operations.get(i) == "XOR"){
                return i;
            }
        }
        return 0;
    }

    public HashMap<Object,Tuple> computeSQLTerm(SQLTerm sqlTerm) throws DBAppException, ClassNotFoundException {
        HashMap<Object, Tuple> dataSet = new HashMap<>();
        if(doesIndexExist(this.tableName,sqlTerm._strColumnName)){
            List<Page> pages = getPages(tableName);
            //Index format: key:value in column , value: page number-primary key as a string
            String type = attributes.get(primaryKey);
            switch (attributes.get(sqlTerm._strColumnName)){
                case "java.lang.String" -> {
                    BTree<String,String> bTree = BTree.deserialize(tableName,sqlTerm._strColumnName);
                    LinkedList<Pointer<String,String>> list = bTree.computeOperator(String.valueOf(sqlTerm._objValue),sqlTerm._strOperator);
                    for(Pointer<String, String> pointer : list){
                        int pageNum = Integer.parseInt(pointer.getValue().split("-")[0]);
                        Object primaryKey = getParsedPrimaryKey(type,pointer.getValue().split("-")[1]);
                        Page page = pages.get(pageNum-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(primaryKey));
                        dataSet.put(primaryKey,tuple);
                    }
                }
                case "java.lang.Integer" -> {
                    BTree<Integer,String> bTree = BTree.deserialize(tableName,sqlTerm._strColumnName);
                    LinkedList<Pointer<Integer,String>> list = bTree.computeOperator((int) sqlTerm._objValue,sqlTerm._strOperator);
                    for(Pointer<Integer, String> pointer : list){
                        int pageNum = Integer.parseInt(pointer.getValue().split("-")[0]);
                        Object primaryKey = getParsedPrimaryKey(type,pointer.getValue().split("-")[1]);
                        Page page = pages.get(pageNum-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(primaryKey));
                        dataSet.put(primaryKey,tuple);
                    }
                }
                case "java.lang.Double" -> {
                    BTree<Double,String> bTree = BTree.deserialize(tableName,sqlTerm._strColumnName);
                    LinkedList<Pointer<Double,String>> list = bTree.computeOperator((double) sqlTerm._objValue,sqlTerm._strOperator);
                    for(Pointer<Double, String> pointer : list){
                        int pageNum = Integer.parseInt(pointer.getValue().split("-")[0]);
                        Object primaryKey = getParsedPrimaryKey(type,pointer.getValue().split("-")[1]);
                        Page page = pages.get(pageNum-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(primaryKey));
                        dataSet.put(primaryKey,tuple);
                    }
                    return dataSet;
                }
            }
        }else{
            return linearSearch(sqlTerm._strColumnName,sqlTerm._strOperator,sqlTerm._objValue);
        }
        return null;
    }
    
    public Object getParsedPrimaryKey(String type,Object val){
        return switch (type){
            case "java.lang.String" -> String.valueOf(val);
            case "java.lang.Integer" -> Integer.parseInt((String) val);
            case "java.lang.Double" -> Double.parseDouble((String) val);
            default -> null;
        };
    }

    private HashMap<Object,Tuple> linearSearch(String colName,String operator,Object value){
        HashMap<Object, Tuple> tupleMap = new HashMap<>();
        List<Page> pages = getPages(tableName);
        for (Page page : pages) {
            for (Tuple tuple : page.getTuples()) {
                switch (operator) {
                    case ">" -> {
                        if (compareValues(tuple, colName, value) > 0) {
                            tupleMap.put(tuple.getPrimaryKeyValue(), tuple);
                        }
                    }
                    case ">=" -> {
                        if (compareValues(tuple, colName, value) >= 0) {
                            tupleMap.put(tuple.getPrimaryKeyValue(), tuple);
                        }
                    }
                    case "<" -> {
                        if (compareValues(tuple, colName, value) < 0) {
                            tupleMap.put(tuple.getPrimaryKeyValue(), tuple);
                        }
                    }
                    case "<=" -> {
                        if (compareValues(tuple, colName, value) <= 0) {
                            tupleMap.put(tuple.getPrimaryKeyValue(), tuple);
                        }
                    }
                    case "!=" -> {
                        if (compareValues(tuple, colName, value) != 0) {
                            tupleMap.put(tuple.getPrimaryKeyValue(), tuple);
                        }
                    }
                    case "=" -> {
                        if (compareValues(tuple, colName, value) == 0) {
                            tupleMap.put(tuple.getPrimaryKeyValue(), tuple);
                        }
                    }
                }
            }
        }
        return tupleMap;
    }
    // Testing, ignore if not needed
    public static void main(String[] args) {

        DBApp dbApp = new DBApp();
        try {
            Hashtable<String,String> htblColNameType = new Hashtable<>();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable( "Student", "id", htblColNameType );
            dbApp.createIndex("Student","gpa","gpaIndex");
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
        String[] names = {"Ahmed","Mohamed","Ali","Omar","Mahmoud"};
        for (int i = 0;i<5;i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", 2343442 + i * 2);
            htblColNameValue.put("name", names[i]);
            htblColNameValue.put("gpa", 0.95 + i);
            try {
                dbApp.insertIntoTable("Student", htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        }

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2343443);
        htblColNameValue.put("name", "Ahmed Noor");
        htblColNameValue.put("gpa", 0.95);
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }


        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("name", "Abdo");
        htblColNameValue.put("gpa", 0.7);
        try {
            dbApp.updateTable("Student", "2343443",  htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        Table table = Serializer.deserializeTable("Student");
        List<Page> pageList = table.getPages("Student");
        for (Page page: pageList) {
            System.out.println("Page Name: Student"+page.getSerial());
            for (int i = 0; i < page.getTuples().size(); i++) {
                System.out.println(page.getTuples().get(i));
            }
        }
        System.out.println("Done");
        BTree<Double, String> bTree= BTree.deserialize("Student","gpa");
        System.out.println(bTree);

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2343444);
        try {
            dbApp.deleteFromTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2343446);
        try {
            dbApp.deleteFromTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2343446);
        htblColNameValue.put("name", "Omar");
        htblColNameValue.put("gpa", 1.95);
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2343451);
        htblColNameValue.put("name", "Omar");
        htblColNameValue.put("gpa", 2.95);
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }
        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2343452);
        htblColNameValue.put("name", "Omar");
        htblColNameValue.put("gpa", 2.95);
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

//        try {
//            System.out.println();
//            htblColNameValue = new Hashtable<>();
//            htblColNameValue.put("name", "Omar");
//            dbApp.deleteFromTable("Student", htblColNameValue);
//        } catch (DBAppException e) {
//            throw new RuntimeException(e);
//        }

        table = Serializer.deserializeTable("Student");
        table.printTable();

        bTree= BTree.deserialize("Student","gpa");
        System.out.println(bTree);
    }

    public void printTable() {
        List<Page> pages = getPages(tableName);
        for (Page page : pages) {
            System.out.println("Page Name: " + tableName + page.getSerial());
            for (Tuple tuple : page.getTuples()) {
                System.out.println(tuple);
            }
        }
    }
}