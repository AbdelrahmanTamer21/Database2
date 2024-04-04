package Main;

import BTree.BTree;
import Utilities.Serializer;
import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import Exception.DBAppException;
import BTree.*;

public class Table implements Serializable {
    private String tableName;
    private String primaryKey;
    private Vector<String> pageNames;
    private LinkedHashMap<String,String> attributes;
    private int size;
    private Vector<BTree<?,?>> indices;

    public Table(String name, String primaryKeyColumn, LinkedHashMap<String, String> attributes) {
        this.tableName = name;
        this.primaryKey = primaryKeyColumn;
        this.pageNames = new Vector<>();
        this.attributes = attributes;
        this.size = 0;
        // Create a directory for the table's pages
        File pagesDir = new File("Pages/"+name);
        pagesDir.mkdirs();
        pagesDir.delete();
        // Create a directory for the table's indices
        File indexDir = new File("Indices/"+name);
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
    // Method to get all the indices names of a table
    private static List<String> getIndicesNames(String tableName){
        List<String> foundFiles = new ArrayList<>();
        File folder = new File("Indices/"+tableName);
        folder.mkdirs();
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < Objects.requireNonNull(listOfFiles).length; i++) {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().endsWith("Index.ser")) {
                foundFiles.add(listOfFiles[i].getName());
            }
        }
        return foundFiles;
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

    private void updateIndices(String strTableName, List<String> bTrees) throws DBAppException {
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);
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
        List<String> bTrees = getIndicesNames(tableName);
        List<Page> pages = getPages(tableName);
        if(pageNames.isEmpty()){
            // Create a new page and insert the new tuple in it
            Page page = new Page(new Vector<>(),tableName,1);
            page.insert(htblColNameValue, attributes);
            pageNames.add(tableName + "1.ser");
            Serializer.serializePage(page,tableName,1);
        }else {
            // Figure out which page to insert the new tuple in
            int serialToInsertIn = findPageToInsert(pages,String.valueOf(htblColNameValue.get(primaryKey)));
            // If the primary key already exists it returns -1, throw an exception
            if(serialToInsertIn == -1){
                throw new DBAppException("Primary key already exists");
            }
            // If the page is full, shift values to other pages, else insert the new tuple in the page
            if(pages.get(serialToInsertIn-1).isFull()){
                shiftValuesToOtherPages(pages,serialToInsertIn,tableName,htblColNameValue);
                updateIndices(tableName, bTrees);
                return;
            }else{
                pages.get(serialToInsertIn-1).insert(htblColNameValue,attributes);
                Serializer.serializePage(pages.get(serialToInsertIn-1),tableName,serialToInsertIn);
            }
        }
        size++;
        int serialToInsertIn = findPageForCertainValue(pages,String.valueOf(htblColNameValue.get(primaryKey)));
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert(String.valueOf(htblColNameValue.get(colName)),serialToInsertIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert(Integer.parseInt(String.valueOf(htblColNameValue.get(colName))),serialToInsertIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.insert(Double.parseDouble(String.valueOf(htblColNameValue.get(colName))),serialToInsertIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    // Method to update a certain tuple
    public void updateTuple(String primaryKey,Hashtable<String,Object> values) throws DBAppException {
        List<Page> pages = getPages(tableName);
        List<String> bTrees = getIndicesNames(tableName);
        int pageToUpdateIn = findPageToUpdateIn(pages, primaryKey);
        Hashtable<String, Object> data = pages.get(pageToUpdateIn-1).update(primaryKey, values, attributes);
        Serializer.serializePage(pages.get(pageToUpdateIn-1),tableName,pages.get(pageToUpdateIn-1).getSerial());
        updateIndices(tableName, bTrees);
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);

                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete(String.valueOf(data.get(colName)),pageToUpdateIn);
                        bTree1.insert(String.valueOf(values.get(colName)),pageToUpdateIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete(Integer.parseInt(String.valueOf(data.get(colName))),pageToUpdateIn);
                        bTree1.insert(Integer.parseInt(String.valueOf(values.get(colName))),pageToUpdateIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete(Double.parseDouble(String.valueOf(data.get(colName))),pageToUpdateIn);
                        bTree1.insert(Double.parseDouble(String.valueOf(values.get(colName))),pageToUpdateIn);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    public void deleteTuples(Hashtable<String, Object> values) throws DBAppException {
        ArrayList<String> results = new ArrayList<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            ArrayList<String> satisfyingTuples = findTuplesSatisfyingCondition(entry);
            if (satisfyingTuples.isEmpty()) {
                return;
            }
            if (results.isEmpty()) {
                results = satisfyingTuples;
            } else {
                results = intersect(results, satisfyingTuples);
            }
        }
        for (String result : results) {
            String[] split = result.split("-");
            deleteTuple(split[0]);
        }
    }

    private ArrayList<String> findTuplesSatisfyingCondition(Map.Entry<String,Object> entry) throws DBAppException {
        List<Page> pages = getPages(tableName);
        ArrayList<String> satisfyingTuples = new ArrayList<>();
        if(doesIndexExist(tableName,entry.getKey())){
            switch (attributes.get(entry.getKey())) {
                case "java.lang.String" -> {
                    BTree<String, Integer> bTree = BTree.deserialize(tableName, entry.getKey());
                }
                case "java.lang.Integer" -> {
                    BTree<Integer, Integer> bTree = BTree.deserialize(tableName, entry.getKey());
                }
                case "java.lang.Double" -> {
                    BTree<Double, Integer> bTree = BTree.deserialize(tableName, entry.getKey());
                }
            }
        }else {
            int i = getIndexInAttributes(entry.getKey());
            for (Page page : pages) {
                for (Tuple tuple : page.getTuples()) {
                    if (tuple.getValues()[i].equals(String.valueOf(entry.getValue()))) {
                        satisfyingTuples.add(tuple.getValues()[0] + "-" + page.getSerial());
                    }
                }
            }
        }
        return satisfyingTuples;
    }

    private ArrayList<String> intersect(ArrayList<String> list1, ArrayList<String> list2){
        ArrayList<String> result = new ArrayList<>();
        for (String s : list1) {
            if(list2.contains(s)){
                result.add(s);
            }
        }
        return result;
    }

    // Method to delete a tuple
    public void deleteTuple(String primaryKeyVal) throws DBAppException {
        List<Page> pages = getPages(tableName);
        List<String> bTrees = getIndicesNames(tableName);
        int pageToDeleteFrom = findPageToDeleteFrom(pages, primaryKeyVal);
        Page page = pages.get(pageToDeleteFrom-1);
        Vector<Tuple> tuples = page.getTuples();
        Tuple tuple = null;
        if(tuples.size() > 1){
            tuple = page.delete(primaryKeyVal);
            Serializer.serializePage(page,tableName,page.getSerial());
        }else{
            if(tuples.size() != 0 && tuples.get(0).getValues()[0].compareTo(primaryKeyVal) == 0){
                tuple = page.delete(primaryKeyVal);
                List<String> pageNamesList = new LinkedList<>(pageNames);
                pageNamesList = pageNamesList.subList(pageToDeleteFrom-1,pageNamesList.size());
                File file = new File("Pages/" + tableName + "/" + tableName + pages.size()+".ser");
                File folder = new File("Pages/"+tableName);
                File[] listOfFiles = folder.listFiles();
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
        pageToDeleteFrom = findPageForCertainValue(pages, primaryKeyVal);
        if(!bTrees.isEmpty()){
            for (String bTree : bTrees) {
                String colName = bTree.replace("Index.ser","");
                String type = attributes.get(colName);
                int i = getIndexInAttributes(colName);
                switch (type){
                    case "java.lang.String" -> {
                        BTree<String, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete(tuple.getValues()[i],pageToDeleteFrom);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Integer" -> {
                        BTree<Integer, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete(Integer.parseInt(tuple.getValues()[i]),pageToDeleteFrom);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                    case "java.lang.Double" -> {
                        BTree<Double, Integer> bTree1 = BTree.deserialize(tableName,colName);
                        bTree1.delete(Double.parseDouble(tuple.getValues()[i]),pageToDeleteFrom);
                        BTree.serialize(bTree1,tableName,colName);
                    }
                }
            }
        }
    }

    public int getIndexInAttributes(String key){
        int i = 0;
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            if(entry.getKey().equals(key)){
                return i;
            }
            i++;
        }
        return -1;
    }

    // return the serial of the page to delete the tuple from
    public static int findPageToDeleteFrom(List<Page> pages, String primaryKey){
        int low = 1;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = pages.get(mid);

            // Compare the new string with the first value in the vector of current page
            String[] firstValue = currentPage.getTuples().get(0).getValues();
            int comparisonResult = primaryKey.compareTo(firstValue[0]);
            // If primaryKey is less than or equal to the first value, go left
            if (comparisonResult < 0) {
                return mid;
            }
            // If primaryKey is greater, go right
            else {
                low = mid + 1;
            }
        }
        // If not found, return the last page
        return pages.size();
    }
    // return the serial of the page where the tuple to be updated is at
    public static int findPageToUpdateIn(List<Page> pages, String primaryKey){
        int low = 1;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = pages.get(mid);

            if(currentPage.isEmpty()){
                return mid+1;
            }
            // Compare the new string with the first value in the vector of current page
            String[] firstValue = currentPage.getTuples().get(0).getValues();
            int comparisonResult = primaryKey.compareTo(firstValue[0]);

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

    // return the serial of the page to insert the new tuple in
    public static int findPageToInsert(List<Page> pages, String newPrimaryKey){
        int low = 1;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = pages.get(mid);

            if(currentPage.isEmpty()){
                return mid+1;
            }
            // Compare the new string with the first value in the vector of current page
            String[] firstValue = currentPage.getTuples().get(0).getValues();
            int comparisonResult = newPrimaryKey.compareTo(firstValue[0]);

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

    public static int findPageForCertainValue(List<Page> pages, String primaryKey){
        int low = 1;
        int high = pages.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Page currentPage = pages.get(mid);

            if(currentPage.isEmpty()){
                return mid+1;
            }
            // Compare the new string with the first value in the vector of current page
            String[] firstValue = currentPage.getTuples().get(0).getValues();
            int comparisonResult = primaryKey.compareTo(firstValue[0]);

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

    // Method to shift values to other pages if there's no space
    public void shiftValuesToOtherPages(List<Page> pages, int serial, String tableName, Hashtable<String, Object> values) throws DBAppException {
        List<Page> newPages = pages.subList(serial-1, pages.size());

        // If the value to be inserted is larger than the last value in the page create a new page and shift the values from other pages to it
        if(pages.get(serial-1).getLastTuple().getValues()[0].compareTo(String.valueOf(values.get(primaryKey))) < 0){
            if(serial >= pages.size()){
                newPages = new LinkedList<>();
            }else {
                newPages = pages.subList(serial, pages.size());
            }
        }

        for (Page page : newPages) {
            if (!page.isFull()) {
                page.insert(values,attributes);
                Serializer.serializePage(page,tableName,page.getSerial());
                return;
            }else{
                String[] lastTupleData = page.removeLastTuple().getValues();
                page.insert(values,attributes);
                Hashtable<String, Object> lastTuple = new Hashtable<>();
                int i = 0;
                for(Map.Entry<String, String> entry : attributes.entrySet()) {
                    String key = entry.getKey();
                    String type = entry.getValue();
                    String data = lastTupleData[i];
                    switch (type) {
                        case "java.lang.Integer" -> lastTuple.put(key, Integer.parseInt(data));
                        case "java.lang.String" -> lastTuple.put(key, data);
                        case "java.lang.Double" -> lastTuple.put(key, Double.parseDouble(data));
                    }
                    i++;
                }
                values = lastTuple;
                Serializer.serializePage(page,tableName,page.getSerial());
            }
        }
        // If no page has space, create a new page and insert the new string
        int newPageId = pages.size() + 1; // Assuming page ids start from 1
        Page newPage = new Page(new Vector<>(),tableName, newPageId); // Assuming max size of 10 for new pages
        newPage.insert(values, attributes);
        Serializer.serializePage(newPage,tableName,newPageId);
        pages.add(newPage);
        pageNames.add(tableName + newPageId + ".ser");
    }

    public void createIndex(String colName, String indexName) throws DBAppException {
        List<Page> pages = getPages(tableName);

        if(attributes.get(colName) == null){
            throw new DBAppException("Wrong column name");
        }
        int index = 0;
        for (Map.Entry<String, String> entry : attributes.entrySet()){
            if(!Objects.equals(entry.getKey(), colName)){
                index++;
            }else {
                break;
            }
        }
        Vector<Pointer<String, Integer>> data = new Vector<>();
        for (Page page: pages){
            for (Tuple tuple : page.getTuples()){
                data.add(new Pointer<String, Integer>(tuple.getValues()[index],page.getSerial()));
            }
        }
        switch (attributes.get(colName)) {
            case "java.lang.String" -> {
                data.sort(new Comparator<>() {
                    @Override
                    public int compare(Pointer o1, Pointer o2) {
                        String s1 = String.valueOf(o1.getKey());
                        String s2 = String.valueOf(o2.getKey());
                        return s1.compareTo(s2);
                    }
                });
                BTree<String, Integer> bTree = new BTree<String, Integer>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<String, Integer> datum : data) {
                    bTree.insert(datum.getKey(), datum.getValue());
                }
                BTree.serialize(bTree,tableName,colName);
            }
            case "java.lang.Integer" -> {
                data.sort(new Comparator<>() {
                    @Override
                    public int compare(Pointer o1, Pointer o2) {
                        Integer i1 = Integer.valueOf((String) o1.getKey());
                        Integer i2 = Integer.valueOf((String) o2.getKey());
                        return i1.compareTo(i2);
                    }
                });
                BTree<Integer, Integer> bTree = new BTree<Integer, Integer>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<String, Integer> datum : data) {
                    bTree.insert(Integer.valueOf(datum.getKey()), datum.getValue());
                }
                BTree.serialize(bTree,tableName,colName);
            }
            case "java.lang.Double" -> {
                data.sort(new Comparator<>() {
                    @Override
                    public int compare(Pointer o1, Pointer o2) {
                        Double d1 = Double.valueOf((String) o1.getKey());
                        Double d2 = Double.valueOf((String) o2.getKey());
                        return d1.compareTo(d2);
                    }
                });
                BTree<Double, Integer> bTree = new BTree<Double, Integer>(indexName,(!Objects.equals(primaryKey, colName)));
                for (Pointer<String, Integer> datum : data) {
                    bTree.insert(Double.valueOf(datum.getKey()), datum.getValue());
                }
                BTree.serialize(bTree,tableName,colName);
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
            throw new RuntimeException(e);
        }
        String names[] = {"Ahmed","Mohamed","Ali","Omar","Mahmoud"};
        for (int i = 0;i<5;i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", Integer.valueOf(2343442+i*2));
            htblColNameValue.put("name", names[i]);
            htblColNameValue.put("gpa", Double.valueOf(0.95+i));
            try {
                dbApp.insertIntoTable("Student", htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        }

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343443));
        htblColNameValue.put("name", "Ahmed Noor");
        htblColNameValue.put("gpa", Double.valueOf(0.95));
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }


        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343443));
        htblColNameValue.put("name", "Abdo");
        htblColNameValue.put("gpa", Double.valueOf(0.7));
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
        htblColNameValue.put("id", Integer.valueOf(2343444));
        try {
            dbApp.deleteFromTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343446));
        try {
            dbApp.deleteFromTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343446));
        htblColNameValue.put("name", "Omar");
        htblColNameValue.put("gpa", Double.valueOf(0.95));
        try {
            dbApp.insertIntoTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }



        table = Serializer.deserializeTable("Student");
        pageList = table.getPages("Student");
        for (Page page: pageList) {
            System.out.println("Page Name: Student"+page.getSerial());
            for (int i = 0; i < page.getTuples().size(); i++) {
                System.out.println(page.getTuples().get(i));
            }
        }
        try {
            ArrayList<String> test = table.findTuplesSatisfyingCondition(new Map.Entry<String, Object>() {
                @Override
                public String getKey() {
                    return "name";
                }

                @Override
                public Object getValue() {
                    return "Omar";
                }

                @Override
                public Object setValue(Object value) {
                    return null;
                }

            });
            System.out.println(test);
            System.out.println();
            htblColNameValue = new Hashtable<>();
            htblColNameValue.put("name", "Omar");
            dbApp.deleteFromTable("Student", htblColNameValue);
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }

        table = Serializer.deserializeTable("Student");
        pageList = table.getPages("Student");
        for (Page page: pageList) {
            System.out.println("Page Name: Student"+page.getSerial());
            for (int i = 0; i < page.getTuples().size(); i++) {
                System.out.println(page.getTuples().get(i));
            }
        }

        BTree<Double, Integer> bTree= BTree.deserialize("Student","gpa");
        System.out.println(bTree);
    }
}