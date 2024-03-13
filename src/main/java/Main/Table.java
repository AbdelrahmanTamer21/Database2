package Main;

import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import Exception.DBAppException;

public class Table implements Serializable {
    private final String tableName;
    private transient LinkedHashMap<String,String> attributes;

    public Table(String name, LinkedHashMap<String, String> attributes) {
        this.tableName = name;
        this.attributes = attributes;
        // Create a directory for the table's pages
        File pagesDir = new File("Pages/"+name);
        pagesDir.mkdirs();
        // Create a directory for the table's indices
        File indexDir = new File("Indices/"+name);
        indexDir.mkdirs();
    }

    // Method to get all the page names of a table
    public static List<String> getPageNames(String tableName){
        List<String> foundFiles = new ArrayList<>();
        File folder = new File("Pages/"+tableName);
        folder.mkdirs();
        File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < Objects.requireNonNull(listOfFiles).length; i++) {
            if (listOfFiles[i].isFile() && listOfFiles[i].getName().startsWith(tableName)) {
                foundFiles.add(listOfFiles[i].getName());
            }
        }
        return foundFiles;
    }
    // Method to get the attributes of a table
    public static LinkedHashMap<String,String> getAttributes(String strTableName){
        LinkedHashMap<String,String> attr = new LinkedHashMap<>();
        String csvFile = "metadata.csv";
        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String[] currLine;
            while ((currLine = reader.readNext()) != null) {
                // Process each row of the CSV file
                if(Objects.equals(currLine[0], strTableName)){
                    attr.put(currLine[1],currLine[2]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return attr;
    }
    // Method to get all the actual pages of a table
    public static List<Page> getPages(String tableName){
        List<Page> pages = new ArrayList<>();
        List<String> pageNames = getPageNames(tableName);
        pageNames.sort(String::compareTo);
        for (String pageName : pageNames) {
            pages.add(Page.deserialize(tableName,Integer.parseInt( pageName.substring(tableName.length(),pageName.length()-4) ) ) );
        }
        return pages;
    }

    // Method to insert a new tuple in the table
    public static void insertTuple(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        List<String> pageNames = getPageNames(strTableName);
        if(pageNames.isEmpty()){
            // Create a new page and insert the new tuple in it
            Page page = new Page(new Vector<>(),strTableName,1);
            page.insert(htblColNameValue,Table.getAttributes(strTableName));
            Page.serialize(page,strTableName,1);
        }else if(pageNames.size() == 1 && !Objects.requireNonNull(Page.deserialize(strTableName, 1)).isFull()){
            // If there's only one page and it's not full, insert the new tuple in it
            Page page = Page.deserialize(strTableName,1);
            page.insert(htblColNameValue,Table.getAttributes(strTableName));
            Page.serialize(page,strTableName,1);
        }else {
            List<Page> pages = getPages(strTableName);
            // Figure out which page to insert the new tuple in
            int serialToInsertIn = findPageToInsert(pages,String.valueOf(htblColNameValue.get("id")));
            // If the primary key already exists it returns -1, throw an exception
            if(serialToInsertIn == -1){
                throw new DBAppException("Primary key already exists");
            }
            // If the page is full, shift values to other pages, else insert the new tuple in the page
            if(pages.get(serialToInsertIn-1).isFull()){
                shiftValuesToOtherPages(pages,serialToInsertIn,strTableName,htblColNameValue);
            }else{
                pages.get(serialToInsertIn-1).insert(htblColNameValue,getAttributes(strTableName));
                Page.serialize(pages.get(serialToInsertIn-1),strTableName,serialToInsertIn);
            }
        }


    }

    public static void updateTuple(String strTableName, String primaryKey,Hashtable<String,Object> values) throws DBAppException {
        List<Page> pages = getPages(strTableName);
        int pageToUpdateIn = findPageToUpdateIn(pages, primaryKey);
        pages.get(pageToUpdateIn-1).update(primaryKey, values);
        Page.serialize(pages.get(pageToUpdateIn-1),strTableName,pages.get(pageToUpdateIn-1).getSerial());
    }
    public static String getPrimaryKey(String strTableName) throws DBAppException {
        try{
            FileReader fileReader = new FileReader("metadata.csv");

            CSVReader csvReader = new CSVReader(fileReader);

            String[] values;
            while ((values = csvReader.readNext()) != null) {
                if(Objects.equals(values[0], strTableName)){
                    if(Objects.equals(values[3], "true")) {
                        return values[1];
                    }else throw new DBAppException("Main.Table doesn't exist");
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }
    public static void deleteTuple(String strTableName,Hashtable<String,Object> values ) throws DBAppException {
        List<Page> pages = getPages(strTableName);
        String primaryKey = getPrimaryKey(strTableName);
        int pageToDeleteFrom = findPageToDeleteFrom(pages, String.valueOf(values.get(primaryKey)));
        Page page = pages.get(pageToDeleteFrom-1);
        Vector<Tuple> tuples = page.getTuples();
        if(tuples.size() > 1){
            page.delete(String.valueOf(values.get(primaryKey)));
            Page.serialize(page,strTableName,page.getSerial());
        }else{
            if(tuples.size() != 0 && tuples.get(0).getValues()[0].compareTo(String.valueOf(values.get(primaryKey))) == 0){
                page.delete(String.valueOf(values.get(primaryKey)));
                List<String> pageNames = getPageNames(strTableName);
                pageNames = pageNames.subList(pageToDeleteFrom-1,pageNames.size());
                File file = new File("Pages/" + strTableName + "/" + strTableName + pages.size()+".ser");
                File folder = new File("Pages/"+strTableName);
                File[] listOfFiles = folder.listFiles();
                listOfFiles[pageToDeleteFrom-1].delete();
                for (int i = pageToDeleteFrom-1; i < pageNames.size(); i++) {
                    pages.get(i+1).setSerial(i+1);
                    Page.serialize(pages.get(i+1),strTableName, i+1);
                }
                file.delete();
            }
        }

    }
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

    //return the serial of the page to insert the new tuple in
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


    // Method to shift values to other pages if there's no space
    public static void shiftValuesToOtherPages(List<Page> pages, int serial, String tableName, Hashtable<String, Object> values) throws DBAppException {
        List<Page> newPages = pages.subList(serial-1, pages.size());
        LinkedHashMap<String, String> attributes = getAttributes(tableName);
        String primaryKey = getPrimaryKey(tableName);

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
                Page.serialize(page,tableName,page.getSerial());
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
                Page.serialize(page,tableName,page.getSerial());
            }
        }
        // If no page has space, create a new page and insert the new string
        int newPageId = pages.size() + 1; // Assuming page ids start from 1
        Page newPage = new Page(new Vector<>(),tableName, newPageId); // Assuming max size of 10 for new pages
        newPage.insert(values,getAttributes(tableName));
        Page.serialize(newPage,tableName,newPageId);
        pages.add(newPage);
    }

    // Method to delete a tuple from the table


    // Testing, ignore if not needed
    public static void main(String[] args) {

        DBApp dbApp = new DBApp();

        for (int i = 0;i<5;i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", Integer.valueOf(2343442+i*2));
            htblColNameValue.put("name", "Ahmed Noor");
            htblColNameValue.put("gpa", Double.valueOf(0.95+i));
            try {
                insertTuple("Student", htblColNameValue);
            } catch (DBAppException e) {
                e.printStackTrace();
            }
        }

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343443));
        htblColNameValue.put("name", "Ahmed Noor");
        htblColNameValue.put("gpa", Double.valueOf(0.95));
        try {
            insertTuple("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }


        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343443));
        htblColNameValue.put("name", "Ahmed Ahmed");
        htblColNameValue.put("gpa", Double.valueOf(0.7));
        try {
            updateTuple("Student", "2343443",  htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        List<Page> pageList = getPages("Student");
        for (Page page: pageList) {
            System.out.println("Page Name: Student"+page.getSerial());
            for (int i = 0; i < page.getTuples().size(); i++) {
                System.out.println(page.getTuples().get(i));
            }
        }
        System.out.println("Done");

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343444));
        htblColNameValue.put("name", "Ahmed Noor");
        htblColNameValue.put("gpa", Double.valueOf(0.95));
        try {
            deleteTuple("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343446));
        htblColNameValue.put("name", "Ahmed Noor");
        htblColNameValue.put("gpa", Double.valueOf(0.95));
        try {
            deleteTuple("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", Integer.valueOf(2343446));
        htblColNameValue.put("name", "Ahmed Noor");
        htblColNameValue.put("gpa", Double.valueOf(0.95));
        try {
            insertTuple("Student", htblColNameValue);
        } catch (DBAppException e) {
            e.printStackTrace();
        }

        pageList = getPages("Student");
        for (Page page: pageList) {
            System.out.println("Page Name: Student"+page.getSerial());
            for (int i = 0; i < page.getTuples().size(); i++) {
                System.out.println(page.getTuples().get(i));
            }
        }
    }
}