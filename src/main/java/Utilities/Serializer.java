package Utilities;

import Main.Page;
import Main.Table;

import java.io.*;

public class Serializer {

    // Method to serialize the page
    public static void serializePage(Page page, String tableName, int serial) {
        try {
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
    public static Page deserializePage(String tableName,int serial){
        try {
            FileInputStream fileInputStream = new FileInputStream ("Pages/" + tableName + "/" + tableName + serial +".ser");

            ObjectInputStream objInputStream = new ObjectInputStream (fileInputStream);

            Page page = (Page) objInputStream.readObject();

            objInputStream.close();
            fileInputStream.close();

            return page;

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // Method to serialize the table
    public static void serializeTable(Table table, String tableName) {
        try {
            //you may also write this verbosely as
            FileOutputStream fileOutputStream = new FileOutputStream("Tables/" + tableName + ".ser");

            ObjectOutputStream objOutputStream = new ObjectOutputStream(fileOutputStream);

            objOutputStream.writeObject(table);
            //we don't want a memory leak if we can avoid it
            fileOutputStream.close();
            objOutputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to deserialize the page
    public static Table deserializeTable(String tableName){
        try {
            FileInputStream fileInputStream = new FileInputStream ("Tables/" + tableName +".ser");

            ObjectInputStream objInputStream = new ObjectInputStream (fileInputStream);

            Table table = (Table) objInputStream.readObject();

            objInputStream.close();
            fileInputStream.close();

            return table;

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
