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

    public Table(String name, String primaryKeyColumn, LinkedHashMap<String, String> attributes) {
        this.tableName = name;
        this.primaryKey = primaryKeyColumn;
        this.pageNames = new Vector<>();
        this.attributes = attributes;
        this.size = 0;
        bTrees = new Vector<>();
        // Create a directory for the table's pages
        File pagesDir = new File("Pages/"+name);
        pagesDir.delete();
        pagesDir.mkdirs();
        // Create a directory for the table's indices
        File indexDir = new File("Indices/"+name);
        indexDir.delete();
        indexDir.mkdirs();
    }

    /**
     * Helper Methods
     */
    // Method to get all the actual pages of a table
    private List<Page> getPages(String tableName){
        List<Page> pages = new ArrayList<>();
        pageNames.sort(String::compareTo);
        for (String pageName : pageNames) {
            pages.add(Serializer.deserializePage(tableName,Integer.parseInt( pageName.substring(tableName.length(),pageName.length()-4) ) ) );
        }
        return pages;
    }
    private static boolean doesIndexExist(String tableName, String colName){
        List<String> foundFiles = new ArrayList<>();
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

                createIndex(colName, bTree1.getIndexName());
            }
        }
    }

    /**
     * Main methods
     */
    // Method to insert a new tuple in the table
    public void insertTuple(Hashtable<String,Object> htblColNameValue) throws DBAppException {
        List<Page> pages = getPages(tableName);
        if(pageNames.isEmpty()){
            // Create a new page and insert the new tuple in it
            Page page = new Page(new Vector<>(),1);
            page.insert(htblColNameValue, attributes, primaryKey);
            pageNames.add(tableName + "1.ser");
            Serializer.serializePage(page,tableName,1);
        }else {
            // Figure out which page to insert the new tuple in
            int serialToInsertIn = findPageToInsert(pages,htblColNameValue.get(primaryKey));
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
                        BTree<String, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert((String) htblColNameValue.get(colName),serialToInsertIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert((int) htblColNameValue.get(colName),serialToInsertIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert((double) htblColNameValue.get(colName),serialToInsertIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    // Method to update a certain tuple
    public void updateTuple(String primaryKey,Hashtable<String,Object> values) throws DBAppException {
        List<Page> pages = getPages(tableName);
        int pageToUpdateIn;
        Hashtable<String, Object> data;
        switch (attributes.get(this.primaryKey)){
            case "java.lang.String" -> {
                pageToUpdateIn = findPageForCertainValue(pages, primaryKey);
                data = pages.get(pageToUpdateIn-1).update(primaryKey, values, attributes);
            }
            case "java.lang.Integer" -> {
                pageToUpdateIn = findPageForCertainValue(pages, Integer.parseInt(primaryKey));
                data = pages.get(pageToUpdateIn-1).update(Integer.parseInt(primaryKey), values, attributes);
            }
            case "java.lang.Double" -> {
                pageToUpdateIn = findPageForCertainValue(pages, Double.parseDouble(primaryKey));
                data = pages.get(pageToUpdateIn-1).update(Double.parseDouble(primaryKey), values, attributes);
            }
            default -> throw new IllegalStateException("Unexpected value: " + attributes.get(this.primaryKey));
        }
        Serializer.serializePage(pages.get(pageToUpdateIn-1),tableName,pages.get(pageToUpdateIn-1).getSerial());

        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((String)data.get(colName),pageToUpdateIn);
                        bTree1.insert((String) values.get(colName),pageToUpdateIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((int) data.get(colName),pageToUpdateIn);
                        bTree1.insert((int) values.get(colName),pageToUpdateIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((double) data.get(colName),pageToUpdateIn);
                        bTree1.insert((double) values.get(colName),pageToUpdateIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    // Method to delete tuples
    public void deleteTuples(Hashtable<String, Object> values) throws DBAppException {
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
                    BTree<String, Integer> bTree = BTree.deserialize(tableName, entry.getKey());
                    LinkedList<Pointer<String, Integer>> pointers = bTree.searchByRange((String) entry.getValue(), (String) entry.getValue());
                    for (Pointer<String, Integer> pointer : pointers) {
                        Page page = pages.get(pointer.getValue()-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchByCol(entry.getValue(),entry.getKey()));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
                case "java.lang.Integer" -> {
                    BTree<Integer, Integer> bTree = BTree.deserialize(tableName, entry.getKey());
                    LinkedList<Pointer<Integer, Integer>> pointers = bTree.searchByRange((Integer) entry.getValue(), (Integer) entry.getValue());
                    for (Pointer<Integer, Integer> pointer : pointers) {
                        Page page = pages.get(pointer.getValue()-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchByCol(entry.getValue(),entry.getKey()));
                        satisfyingTuples.add(tuple.getPrimaryKeyValue());
                    }
                }
                case "java.lang.Double" -> {
                    BTree<Double, Integer> bTree = BTree.deserialize(tableName, entry.getKey());
                    LinkedList<Pointer<Double, Integer>> pointers = bTree.searchByRange((Double) entry.getValue(), (Double) entry.getValue());
                    for (Pointer<Double, Integer> pointer : pointers) {
                        Page page = pages.get(pointer.getValue()-1);
                        Tuple tuple = page.getTuples().get(page.binarySearchByCol(entry.getValue(),entry.getKey()));
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
                for (int i = pageToDeleteFrom-1; i < pageNamesList.size(); i++) {
                    pages.get(i+1).setSerial(i+1);
                    Serializer.serializePage(pages.get(i+1),tableName, i+1);
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
                        BTree<String, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((String) tuple.getValues().get(colName),pageToDeleteFrom);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((int) tuple.getValues().get(colName),pageToDeleteFrom);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete((double) tuple.getValues().get(colName),pageToDeleteFrom);
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
        return pages.size();
    }

    private int comparePrimaryKey(Object primaryKey, Tuple tuple) {
         return switch (attributes.get(this.primaryKey)) {
            case "java.lang.Integer" -> Integer.compare((Integer) primaryKey, (int) tuple.getPrimaryKeyValue());
            case "java.lang.String" -> (String.valueOf(primaryKey).compareTo((String) tuple.getPrimaryKeyValue()));
            case "java.lang.Double" -> Double.compare((Double) primaryKey, (double) tuple.getPrimaryKeyValue());
            default -> throw new IllegalStateException("Unexpected value: " + tuple.getPrimaryKeyValue().getClass().getSimpleName());
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
                Hashtable<String, Object> lastTupleData = page.removeLastTuple().getValues();
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

    public void createIndex(String colName, String indexName) throws DBAppException {
        List<Page> pages = getPages(tableName);

        if(attributes.get(colName) == null){
            throw new DBAppException("Wrong column name");
        }

        switch (attributes.get(colName)) {
            case "java.lang.String" -> {
                Vector<Pointer<String, Integer>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((String) tuple.getValues().get(colName), page.getSerial()));
                    }
                }
                data.sort((o1, o2) -> {
                    String s1 = String.valueOf(o1.getKey());
                    String s2 = String.valueOf(o2.getKey());
                    return s1.compareTo(s2);
                });
                BTree<String, Integer> bTree = new BTree<String, Integer>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<String, Integer> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                BTree.serialize(bTree,tableName,colName);
            }
            case "java.lang.Integer" -> {
                Vector<Pointer<Integer, Integer>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((Integer) tuple.getValues().get(colName), page.getSerial()));
                    }
                }
                data.sort((o1, o2) -> {
                    Integer i1 = Integer.valueOf(o1.getKey());
                    Integer i2 = Integer.valueOf(o2.getKey());
                    return i1.compareTo(i2);
                });
                BTree<Integer, Integer> bTree = new BTree<Integer, Integer>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<Integer, Integer> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                BTree.serialize(bTree,tableName,colName);
            }
            case "java.lang.Double" -> {
                Vector<Pointer<Double, Integer>> data = new Vector<>();
                for (Page page: pages){
                    for (Tuple tuple : page.getTuples()){
                        data.add(new Pointer<>((Double) tuple.getValues().get(colName), page.getSerial()));
                    }
                }
                data.sort((o1, o2) -> {
                    Double d1 = o1.getKey();
                    Double d2 = o2.getKey();
                    return d1.compareTo(d2);
                });
                BTree<Double, Integer> bTree = new BTree<Double, Integer>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<Double, Integer> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                BTree.serialize(bTree,tableName,colName);
            }
        }
        if(!bTrees.contains(colName+"Index.ser")) {
            bTrees.add(colName+"Index.ser");
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
        htblColNameValue.put("id", 2343443);
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
        htblColNameValue.put("gpa", 0.95);
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }



        table = Serializer.deserializeTable("Student");
        table.printTable();

        /*
        try {
            System.out.println();
            htblColNameValue = new Hashtable<>();
            htblColNameValue.put("name", "Omar");
            htblColNameValue.put("gpa", 0.95);
            dbApp.deleteFromTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
         */

        BTree<Double, Integer> bTree= BTree.deserialize("Student","gpa");
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