package Utilities;

import Main.Page;
import Main.Table;
import BTree.BTree;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Serializer {

    // Method to serialize the page
    public static void serializePage(Page page, String tableName, int serial) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("Pages/" + tableName + "/" + tableName + serial + ".ser");

            ObjectOutputStream objOutputStream = new ObjectOutputStream(new GZIPOutputStream(fileOutputStream));

            objOutputStream.writeObject(page);
            //we don't want a memory leak if we can avoid it
            objOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to deserialize the page
    public static Page deserializePage(String tableName,int serial){
        try {
            FileInputStream fileInputStream = new FileInputStream ("Pages/" + tableName + "/" + tableName + serial +".ser");

            ObjectInputStream objInputStream = new ObjectInputStream(new GZIPInputStream(fileInputStream));

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

            ObjectOutputStream objOutputStream = new ObjectOutputStream(new GZIPOutputStream(fileOutputStream));

            objOutputStream.writeObject(table);
            //we don't want a memory leak if we can avoid it
            objOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to deserialize the page
    public static Table deserializeTable(String tableName){
        try {
            FileInputStream fileInputStream = new FileInputStream ("Tables/" + tableName +".ser");

            ObjectInputStream objInputStream = new ObjectInputStream (new GZIPInputStream(fileInputStream));

            Table table = (Table) objInputStream.readObject();

            objInputStream.close();
            fileInputStream.close();

            return table;

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // Method to serialize the btree
    public static void serializeBTree(BTree<?,String> bTree, String tableName, String indexName) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream("Indices/" + tableName + "/" + indexName + ".ser");

            ObjectOutputStream objOutputStream = new ObjectOutputStream(new GZIPOutputStream(fileOutputStream));

            objOutputStream.writeObject(bTree);
            //we don't want a memory leak if we can avoid it
            objOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Method to deserialize the btree
    public static BTree<?,String> deserializeBTree(String tableName, String indexName){
        try {
            FileInputStream fileInputStream = new FileInputStream ("Indices/" + tableName + "/" + indexName +".ser");

            ObjectInputStream objInputStream = new ObjectInputStream(new GZIPInputStream(fileInputStream));

            BTree<?,String> bTree = (BTree<?,String>) objInputStream.readObject();

            objInputStream.close();
            fileInputStream.close();

            return bTree;

        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
