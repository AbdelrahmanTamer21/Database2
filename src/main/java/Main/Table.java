package Main;

import BTree.BTree;
import BTree.Pointer;
import Exception.DBAppException;
import Utilities.Serializer;

import java.io.File;
import java.io.Serializable;
import java.util.*;

@SuppressWarnings("unchecked")
public class Table implements Serializable {
    private final String tableName;
    private final String primaryKey;
    private final LinkedHashMap<String,String> attributes;
    private int size;
    private final Vector<String> pageNames;
    private final Vector<String> bTrees;
    private final Vector<String> indexNames;
    private final Vector<BTree<?,String>> indices;

    public Table(String name, String primaryKeyColumn, LinkedHashMap<String, String> attributes) {
        this.tableName = name;
        this.primaryKey = primaryKeyColumn;
        this.pageNames = new Vector<>();
        this.attributes = attributes;
        this.size = 0;
        bTrees = new Vector<>();
        indexNames = new Vector<>();
        indices = new Vector<>();
        File pagesDir = new File("Pages/" + tableName);
        pagesDir.mkdirs();
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
        for(int i = 0;i<bTrees.size();i++){
            bTreesList.add(indices.get(i));
        }
        return bTreesList;
    }
    public BTree<?, String> getBTree(String colName) {
        if(bTrees.contains(colName+"Index")){
            for (int i = 0;i<bTrees.size();i++){
                if((colName + "Index").equals(bTrees.get(i))){
                    return indices.get(i);
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
    public void updateBTreeIndex(String colName,BTree<?,String> newBTree){
        if(bTrees.contains(colName+"Index")){
            for (int i = 0;i<bTrees.size();i++){
                if((colName + "Index").equals(bTrees.get(i))){
                    indices.set(i,newBTree);
                }
            }
        }
    }
    private boolean doesIndexExist(String colName){
        return bTrees.contains(colName+"Index");
    }
    public Vector<String> getPageNames() {
        return pageNames;
    }
    public Vector<String> getIndices(){
        return bTrees;
    }
    public Vector<String> getIndexNames(){
        return indexNames;
    }
    public String getPrimaryKey() {
        return primaryKey;
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
        int serialToInsertIn;
        if(pageNames.isEmpty()){
            // Create a new page and insert the new tuple in it
            Page page = new Page(new Vector<>(),1);
            page.insert(htblColNameValue, attributes, primaryKey);
            pageNames.add(tableName + "1.ser");
            Serializer.serializePage(page,tableName,1);
            serialToInsertIn = 1;
        }else {
            // Figure out which page to insert the new tuple in
            if(doesIndexExist(primaryKey)){
                serialToInsertIn = switch (attributes.get(primaryKey)){
                    case "java.lang.String" -> ((BTree<String,String>)getBTree(primaryKey)).getPageNumberForInsert((String) htblColNameValue.get(primaryKey));
                    case "java.lang.Integer" -> ((BTree<Integer,String>)getBTree(primaryKey)).getPageNumberForInsert((Integer) htblColNameValue.get(primaryKey));
                    case "java.lang.Double" -> ((BTree<Double,String>)getBTree(primaryKey)).getPageNumberForInsert((Double) htblColNameValue.get(primaryKey));
                    default -> 0;
                };
            }else {
                serialToInsertIn = findPageForCertainValue(htblColNameValue.get(primaryKey));
            }
            Page page = Serializer.deserializePage(tableName,serialToInsertIn);
            assert page != null;
            // If the primary key already exists, it returns -1, throw an exception
            if(page.binarySearchString(htblColNameValue.get(primaryKey)) == -1){
                throw new DBAppException("Primary key already exists");
            }
            // If the page is full, shift values to other pages, else insert the new tuple in the page
            if(page.isFull()){
                shiftValuesToOtherPages(serialToInsertIn,tableName,htblColNameValue);
                size++;
                return;
            }else{
                page.insert(htblColNameValue,attributes, primaryKey);
                Serializer.serializePage(page,tableName,serialToInsertIn);
            }
        }
        size++;
        //int serialToInsertIn = findPageForCertainValue(pages,htblColNameValue.get(primaryKey));
        if(!bTrees.isEmpty()){
            for (int i = 0;i<bTrees.size();i++) {
                String bTree = bTrees.get(i);
                String colName = bTree.replace("Index","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = (BTree<String, String>) indices.get(i);
                        bTree1.insert((String) htblColNameValue.get(colName),serialToInsertIn + "-" + htblColNameValue.get(primaryKey));
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = (BTree<Integer, String>) indices.get(i);
                        bTree1.insert((int) htblColNameValue.get(colName),serialToInsertIn + "-" + htblColNameValue.get(primaryKey));
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = (BTree<Double, String>) indices.get(i);
                        bTree1.insert((double) htblColNameValue.get(colName),serialToInsertIn + "-" + htblColNameValue.get(primaryKey));
                    }
                }
            }
        }
    }

    // Method to shift values to other pages if there's no space
    public void shiftValuesToOtherPages(int serial, String tableName, Hashtable<String, Object> values) throws DBAppException {
        int index = serial;
        Page checkPage = Serializer.deserializePage(tableName,serial);
        assert checkPage != null;
        Object primaryKeyVal = checkPage.getLastTuple().getPrimaryKeyValue();
        // If the value to be inserted is larger than the last value in the page,
        // create a new page and shift the values from other pages to it
        switch (attributes.get(primaryKey)){
            case "java.lang.String" -> {
                if(String.valueOf(primaryKeyVal).compareTo(String.valueOf(values.get(primaryKey))) < 0){
                    if(serial <= pageNames.size()){
                        index++;
                    }
                }
            }
            case "java.lang.Integer" -> {
                if(Integer.parseInt(primaryKeyVal.toString()) < Integer.parseInt(String.valueOf(values.get(primaryKey)))){
                    if(serial <= pageNames.size()){
                        index++;
                    }
                }
            }
            case "java.lang.Double" -> {
                if(Double.parseDouble(primaryKeyVal.toString()) < Double.parseDouble(String.valueOf(values.get(primaryKey)))){
                    if(serial <= pageNames.size()){
                        index++;
                    }
                }
            }
        }

        for (int i = index;i<=pageNames.size();i++) {
            Page page = Serializer.deserializePage(tableName,i);
            if (!Objects.requireNonNull(page).isFull()) {
                if(!bTrees.isEmpty()){
                    for (int j = 0;j<bTrees.size();j++) {
                        String bTree = bTrees.get(j);
                        String colName = bTree.replace("Index","");
                        String type = attributes.get(colName);

                        switch (type){
                            case "java.lang.String" -> {
                                BTree<String, String> bTree1 = (BTree<String, String>) indices.get(j);
                                bTree1.insert((String) values.get(colName),page.getSerial() + "-" + values.get(primaryKey));
                            }
                            case "java.lang.Integer" -> {
                                BTree<Integer, String> bTree1 = (BTree<Integer, String>) indices.get(j);
                                bTree1.insert((int) values.get(colName),page.getSerial() + "-" + values.get(primaryKey));
                            }
                            case "java.lang.Double" -> {
                                BTree<Double, String> bTree1 = (BTree<Double, String>) indices.get(j);
                                bTree1.insert((double) values.get(colName),page.getSerial() + "-" + values.get(primaryKey));
                            }
                        }
                    }
                }
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
                    if(doesIndexExist(key)){
                        switch (attributes.get(key)) {
                            case "java.lang.Integer" -> {
                                BTree<Integer, String> bTree = (BTree<Integer, String>) getBTree(key);
                                bTree.delete((Integer)lastTupleData.get(key),page.getSerial() + "-" + lastTuple.get(primaryKey));
                                bTree.insert((Integer) values.get(key),page.getSerial() + "-" + values.get(primaryKey));
                            }
                            case "java.lang.String" -> {
                                BTree<String, String> bTree = (BTree<String, String>) getBTree(key);
                                bTree.delete((String)lastTupleData.get(key),page.getSerial() + "-" + lastTuple.get(primaryKey));
                                bTree.insert((String) values.get(key),page.getSerial() + "-" + values.get(primaryKey));
                            }
                            case "java.lang.Double" -> {
                                BTree<Double, String> bTree = (BTree<Double, String>) getBTree(key);
                                bTree.delete((Double)lastTupleData.get(key),page.getSerial() + "-" + lastTuple.get(primaryKey));
                                bTree.insert((Double) values.get(key),page.getSerial() + "-" + values.get(primaryKey));
                            }
                        }
                    }
                }
                values = lastTuple;
                Serializer.serializePage(page,tableName,page.getSerial());
            }
        }
        // If no page has space, create a new page and insert the new string
        int newPageId = pageNames.size() + 1; // Assuming page ids start from 1
        Page newPage = new Page(new Vector<>(), newPageId);
        newPage.insert(values, attributes, primaryKey);
        if(!bTrees.isEmpty()){
            for (int i = 0;i<bTrees.size();i++) {
                String bTree = bTrees.get(i);
                String colName = bTree.replace("Index","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = (BTree<String, String>) indices.get(i);
                        bTree1.insert((String) values.get(colName),newPage.getSerial() + "-" + values.get(primaryKey));
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = (BTree<Integer, String>) indices.get(i);
                        bTree1.insert((int) values.get(colName),newPage.getSerial() + "-" + values.get(primaryKey));
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = (BTree<Double, String>) indices.get(i);
                        bTree1.insert((double) values.get(colName),newPage.getSerial() + "-" + values.get(primaryKey));
                    }
                }
            }
        }
        Serializer.serializePage(newPage,tableName,newPageId);
        pageNames.add(tableName + newPageId + ".ser");
    }

    // Method to update a certain tuple
    public void updateTuple(String primaryKey,Hashtable<String,Object> values) throws DBAppException {
        if(values.containsKey(this.primaryKey)){
            throw new DBAppException("The input row wants to change the primary key");
        }
        if(values.size()>attributes.size()-1){
            throw new DBAppException("The Tuple has more columns than the table's columns");
        }
        Object value = switch (attributes.get(this.primaryKey)){
            case "java.lang.String" -> String.valueOf(primaryKey);
            case "java.lang.Integer" -> Integer.parseInt(primaryKey);
            case "java.lang.Double" -> Double.parseDouble(primaryKey);
            default -> throw new IllegalStateException("Unexpected value: " + attributes.get(this.primaryKey));
        };
        int pageToUpdateIn = findPageForCertainValue(value);
        Page page = Serializer.deserializePage(tableName,pageToUpdateIn);
        assert page != null;
        HashMap<String, Object> data = page.update(value, this.primaryKey, values, attributes);
        Serializer.serializePage(page,tableName,page.getSerial());

        if(!bTrees.isEmpty()){
            for (int i = 0;i<bTrees.size();i++) {
                String bTree = bTrees.get(i);
                String colName = bTree.replace("Index","");
                String type = attributes.get(colName);
                if(!values.containsKey(colName)){
                    return;
                }

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = (BTree<String, String>) indices.get(i);
                        bTree1.delete((String)data.get(colName),pageToUpdateIn + "-" + primaryKey);
                        bTree1.insert((String) values.get(colName),pageToUpdateIn + "-" + primaryKey);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = (BTree<Integer, String>) indices.get(i);
                        bTree1.delete((int) data.get(colName),pageToUpdateIn + "-" + primaryKey);
                        bTree1.insert((int) values.get(colName),pageToUpdateIn + "-" + primaryKey);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = (BTree<Double, String>) indices.get(i);
                        bTree1.delete((double) data.get(colName),pageToUpdateIn + "-" + primaryKey);
                        bTree1.insert((double) values.get(colName),pageToUpdateIn + "-" + primaryKey);
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
            if(Objects.equals(entry.getKey(), primaryKey)){
                int pageToDeleteFrom = findPageForCertainValue(entry.getValue());
                Page page = Serializer.deserializePage(tableName,pageToDeleteFrom);
                assert page != null;
                Tuple tuple = page.getTuples().get(page.binarySearchString(entry.getValue()));
                deleteTuple(tuple.getPrimaryKeyValue());
                return;
            }
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
        if(doesIndexExist(entry.getKey())){
            switch (attributes.get(entry.getKey())) {
                case "java.lang.String" -> {
                    BTree<String, String> bTree = (BTree<String, String>) getBTree(entry.getKey());
                    LinkedList<Pointer<String, String>> pointers = bTree.getEqualKeys((String) entry.getValue());
                    for (Pointer<String, String> pointer : pointers) {
                        Page page = pages.get(Integer.parseInt(pointer.value().split("-")[0])-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(Integer.parseInt(pointer.value().split("-")[1])));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
                case "java.lang.Integer" -> {
                    BTree<Integer, String> bTree = (BTree<Integer, String>) getBTree(entry.getKey());
                    LinkedList<Pointer<Integer, String>> pointers = bTree.getEqualKeys((Integer) entry.getValue());
                    for (Pointer<Integer, String> pointer : pointers) {
                        Page page = pages.get(Integer.parseInt(pointer.value().split("-")[0])-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(Integer.parseInt(pointer.value().split("-")[1])));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
                case "java.lang.Double" -> {
                    BTree<Double, String> bTree = (BTree<Double, String>) getBTree(entry.getKey());
                    LinkedList<Pointer<Double, String>> pointers = bTree.getEqualKeys((Double) entry.getValue());
                    for (Pointer<Double, String> pointer : pointers) {
                        Page page = pages.get(Integer.parseInt(pointer.value().split("-")[0])-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(Integer.parseInt(pointer.value().split("-")[1])));
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
        int pageToDeleteFrom = findPageForCertainValue(primaryKeyVal);
        Page page = Serializer.deserializePage(tableName,pageToDeleteFrom);
        assert page != null;
        Vector<Tuple> tuples = page.getTuples();
        Tuple tuple = null;
        boolean updatePageNum = false;
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
                File file = new File("Pages/" + tableName + "/" + tableName + pageNames.size()+".ser");
                File folder = new File("Pages/"+tableName);
                File[] listOfFiles = folder.listFiles();
                assert listOfFiles != null;
                listOfFiles[pageToDeleteFrom-1].delete();
                pageNames.remove(pageNames.size()-1);
                if(pageNames.size() > 1) {
                    for (int i = pageToDeleteFrom - 1; i < pageNamesList.size(); i++) {
                        Page currentPage = Serializer.deserializePage(tableName, i+2);
                        assert currentPage != null;
                        currentPage.setSerial(i + 1);
                        Serializer.serializePage(currentPage, tableName, i + 1);
                    }
                }
                updatePageNum = true;
                file.delete();
            }
        }
        size--;
        assert tuple != null;
        if(!bTrees.isEmpty()){
            for (int i = 0;i<bTrees.size();i++) {
                String bTree = bTrees.get(i);
                String colName = bTree.replace("Index","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, String> bTree1 = (BTree<String, String>) indices.get(i);
                        bTree1.delete((String) tuple.getValues().get(colName),pageToDeleteFrom + "-" + tuple.getPrimaryKeyValue());
                        if(updatePageNum){
                           bTree1.reducePageNumbers(pageToDeleteFrom);
                        }
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, String> bTree1 = (BTree<Integer, String>) indices.get(i);
                        bTree1.delete((int) tuple.getValues().get(colName),pageToDeleteFrom + "-" + tuple.getPrimaryKeyValue());
                        if(updatePageNum){
                            bTree1.reducePageNumbers(pageToDeleteFrom);
                        }
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, String> bTree1 = (BTree<Double, String>) indices.get(i);
                        bTree1.delete((double) tuple.getValues().get(colName),pageToDeleteFrom + "-" + tuple.getPrimaryKeyValue());
                        if(updatePageNum){
                            bTree1.reducePageNumbers(pageToDeleteFrom);
                        }
                    }
                }
            }
        }
    }

    public int findPageForCertainValue(Object primaryKey){
        int low = 1;
        int high = pageNames.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = Serializer.deserializePage(tableName,mid+1);
            assert currentPage != null;
            if(currentPage.isEmpty()){
                return mid+1;
            }
            // Compare the new string with the first value in the vector of the current page
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
        return pageNames.size()==0?1:pageNames.size();
    }

    private int comparePrimaryKey(Object primaryKey, Tuple tuple) {
         return switch (attributes.get(this.primaryKey)) {
            case "java.lang.Integer" -> Integer.compare((Integer) primaryKey, (int) tuple.getPrimaryKeyValue());
            case "java.lang.String" -> (String.valueOf(primaryKey).compareTo((String) tuple.getPrimaryKeyValue()));
            case "java.lang.Double" -> Double.compare((Double) primaryKey, (double) tuple.getPrimaryKeyValue());
            default -> throw new IllegalStateException("Unexpected value: " + tuple.getPrimaryKeyValue().getClass().getSimpleName());
        };
    }

    public void createIndex(String colName, String indexName, boolean update) throws DBAppException {
        if(!update && indexNames.contains(indexName)){
            throw new DBAppException("The index was already created on one of the columns");
        }
        if(attributes.get(colName) == null){
            throw new DBAppException("Wrong column name");
        }

        List<Page> pages = getPages(tableName);
        BTree<?, String> index = new BTree<String, String>(indexName);
        switch (attributes.get(colName)) {
            case "java.lang.String" -> {
                Vector<Pointer<String, String>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((String) tuple.getValues().get(colName), page.getSerial() + "-" + tuple.getPrimaryKeyValue()));
                    }
                }
                data.sort((o1, o2) -> {
                    String s1 = String.valueOf(o1.key());
                    String s2 = String.valueOf(o2.key());
                    return s1.compareTo(s2);
                });
                BTree<String, String> bTree = new BTree<String, String>(indexName);
                for (Pointer<String, String> datum : data) {
                    bTree.insert(datum.key(), datum.value());
                }
                index = bTree;
            }
            case "java.lang.Integer" -> {
                Vector<Pointer<Integer, String>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((Integer) tuple.getValues().get(colName), page.getSerial() + "-" + tuple.getPrimaryKeyValue()));
                    }
                }
                data.sort((o1, o2) -> {
                    Integer i1 = o1.key();
                    Integer i2 = o2.key();
                    return i1.compareTo(i2);
                });
                BTree<Integer, String> bTree = new BTree<Integer, String>(indexName);
                for (Pointer<Integer, String> datum : data) {
                    bTree.insert(datum.key(), datum.value());
                }
                index = bTree;
            }
            case "java.lang.Double" -> {
                Vector<Pointer<Double, String>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((Double) tuple.getValues().get(colName), page.getSerial() + "-" + tuple.getPrimaryKeyValue()));
                    }
                }
                data.sort((o1, o2) -> {
                    Double d1 = o1.key();
                    Double d2 = o2.key();
                    return d1.compareTo(d2);
                });
                BTree<Double, String> bTree = new BTree<Double, String>(indexName);
                for (Pointer<Double, String> datum : data) {
                    bTree.insert(datum.key(), datum.value());
                }
                index = bTree;
            }
        }
        if(!bTrees.contains(colName+"Index")) {
            bTrees.add(colName+"Index");
            indexNames.add(indexName);
            indices.add(index);
        }else {
            updateBTreeIndex(colName,index);
        }

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

    public Iterator<Tuple> selectFromTable(SQLTerm[] arrSQLTerms, String[]  strarrOperators) {
        List<SQLTerm> sqlTerms = new ArrayList<>(Arrays.asList(arrSQLTerms));
        List<String> operations = new ArrayList<>(Arrays.asList(strarrOperators));
        //Primary key value,Whole tuple
        List<HashMap<Object,Tuple>> results = new ArrayList<>();
        if (sqlTerms.isEmpty()) {
            return null;
        }
        for (SQLTerm sqlTerm : sqlTerms) {
            //sqlterms : name, gpa, grade
            //operations: or,and -> or ->
            // name , gpa, grade -> or -> name and gpa -> and -> name and gpa and grade
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
        int orIndex = -1;
        int xorIndex = -1;
        for (int i = 0; i < operations.size(); i++) {
            String operation = operations.get(i);
            switch (operation) {
                case "AND" -> {
                    return i;
                }
                case "OR" -> orIndex = i;
                case "XOR" -> xorIndex = i;
            }
        }
        return orIndex != -1 ? orIndex : xorIndex;
    }

    public HashMap<Object,Tuple> computeSQLTerm(SQLTerm sqlTerm) {
        HashMap<Object, Tuple> dataSet = new HashMap<>();
        if(doesIndexExist(sqlTerm._strColumnName)){
            List<Page> pages = getPages(tableName);
            //Index format: key:value in column , value: page number-primary key as a string
            String type = attributes.get(primaryKey);
            switch (attributes.get(sqlTerm._strColumnName)){
                case "java.lang.String" -> {
                    BTree<String,String> bTree = (BTree<String,String>) getBTree(sqlTerm._strColumnName);
                    LinkedList<Pointer<String,String>> list = bTree.computeOperator(String.valueOf(sqlTerm._objValue),sqlTerm._strOperator);
                    for(Pointer<String, String> pointer : list){
                        int pageNum = Integer.parseInt(pointer.value().split("-")[0]);
                        Object primaryKey = getParsedPrimaryKey(type,pointer.value().split("-")[1]);
                        Page page = pages.get(pageNum-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(primaryKey));
                        dataSet.put(primaryKey,tuple);
                    }
                }
                case "java.lang.Integer" -> {
                    BTree<Integer,String> bTree = (BTree<Integer,String>) getBTree(sqlTerm._strColumnName);
                    LinkedList<Pointer<Integer,String>> list = bTree.computeOperator((int) sqlTerm._objValue,sqlTerm._strOperator);
                    for(Pointer<Integer, String> pointer : list){
                        int pageNum = Integer.parseInt(pointer.value().split("-")[0]);
                        Object primaryKey = getParsedPrimaryKey(type,pointer.value().split("-")[1]);
                        Page page = pages.get(pageNum-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchString(primaryKey));
                        dataSet.put(primaryKey,tuple);
                    }
                }
                case "java.lang.Double" -> {
                    BTree<Double,String> bTree = (BTree<Double,String>) getBTree(sqlTerm._strColumnName);
                    LinkedList<Pointer<Double,String>> list = bTree.computeOperator((double) sqlTerm._objValue,sqlTerm._strOperator);
                    for(Pointer<Double, String> pointer : list){
                        int pageNum = Integer.parseInt(pointer.value().split("-")[0]);
                        Object primaryKey = getParsedPrimaryKey(type,pointer.value().split("-")[1]);
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

    public void printTable() {
        List<Page> pages = getPages(tableName);
        for (Page page : pages) {
            System.out.println("Page Name: " + tableName + page.getSerial());
            for (Tuple tuple : page.getTuples()) {
                System.out.println(tuple);
            }
        }
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
            e.printStackTrace();
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
        assert table != null;
        table.printTable();
        System.out.println("Done");
        BTree<?, String> bTree= table.getBTree("gpa");
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

        table = Serializer.deserializeTable("Student");
        assert table != null;
        table.printTable();

        bTree= table.getBTree("gpa");
        System.out.println(bTree);
    }
}