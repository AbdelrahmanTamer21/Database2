package BTree;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import Exception.DBAppException;
import Main.Page;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements java.io.Serializable {
	private BTreeNode<TKey> root;
	
	public BTree() {
		this.root = new BTreeLeafNode<TKey, TValue>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		leaf.insertKey(key, value);
		
		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n; 
		}
	}
	
	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public TValue search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}
	
	/**
	 * Delete a key and its associated value from the tree.
	 */
	public void delete(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		if (leaf.delete(key) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n; 
		}
	}

	/**
	 * Update the value associated with a key.
	 */
	public void update(TKey key,TValue newValue){
		BTreeLeafNode<TKey, TValue> node = this.findLeafNodeShouldContainKey(key);

		int index = node.search(key);
		node.setValue(index,newValue);
	}

	public LinkedList<Pointer> searchByRange(TKey min, TKey max) throws DBAppException{
		LinkedList<Pointer> list = new LinkedList<Pointer>();
		BTreeLeafNode<TKey, TValue> minNode = this.findLeafNodeShouldContainKey(min);
		if(minNode == null){
			throw new DBAppException("The min key is not found");
		}
		BTreeLeafNode<TKey,TValue> maxNode = this.findLeafNodeShouldContainKey(max);
		if(maxNode == null){
			throw new DBAppException("The max key is not found");
		}
		do{
			for (int i = 0; i < minNode.getKeyCount(); i++) {
				if(minNode.getKey(i).compareTo(min) >= 0 && minNode.getKey(i).compareTo(max) <= 0){
					list.add(new Pointer(minNode.getKey(i),minNode.getValue(i)));
				}
			}
			minNode = minNode.getRightSibling();
		}while (minNode != maxNode.getRightSibling() && minNode != null);
		return list;
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}
		
		return (BTreeLeafNode<TKey, TValue>)node;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreeNode<TKey>> queue = new LinkedList<>();
		queue.offer(root);
		int level = 0;

		while (!queue.isEmpty()) {
			int levelSize = queue.size();

			// Append the level number
			sb.append("Level ").append(level++).append(": ");

			boolean firstNodeInLevel = true; // Track if it's the first node in the level

			for (int i = 0; i < levelSize; i++) {
				BTreeNode<TKey> node = queue.poll();

				// Append node with border
				sb.append("|").append(node.toString().trim());

				if (node.getNodeType() == TreeNodeType.InnerNode) {
					BTreeInnerNode<TKey> innerNode = (BTreeInnerNode<TKey>) node;
					for (int j = 0; j <= innerNode.getKeyCount(); j++) {
						BTreeNode<TKey> child = innerNode.getChild(j);
						if (child != null) {
							queue.offer(child);
						}
					}
				}
			}
			sb.append("|\n");
		}

		return sb.toString();
	}

	public static void serialize(BTree bTree, String tableName) {
		try {
			//you may also write this verbosely as
			// FileOutputStream fileOutputStream = new FileOutputStream(fileName);
			FileOutputStream fileOutputStream = new FileOutputStream( "Indices/" + tableName + "/" + tableName + ".ser");

			ObjectOutputStream objOutputStream = new ObjectOutputStream(fileOutputStream);

			objOutputStream.writeObject(bTree);
			//we don't want a memory leak if we can avoid it
			fileOutputStream.close();
			objOutputStream.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Method to deserialize the page
	public static BTree deserialize(String tableName){
		try {
			FileInputStream fileInputStream = new FileInputStream ("Indices/" + tableName + "/" + tableName +".ser");

			ObjectInputStream objInputStream = new ObjectInputStream (fileInputStream);

			BTree bTree = (BTree) objInputStream.readObject();

			objInputStream.close();
			fileInputStream.close();

			return bTree;

		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args){
		BTree bTree = new BTree();
		//123 is the value in the table while 5,12 is the pointer to the 5th page in the 12th index
		//insert(key,value)
		bTree.insert("10","5,12");
		bTree.insert("11","5,13");
		bTree.insert("12","5,14");
		bTree.insert("13","5,15");
		bTree.insert("14","5,16");
		bTree.insert("15","12");
		bTree.insert("16","12");
		bTree.insert("17","12");
		bTree.insert("18","12");
		bTree.insert("19","12");
		bTree.insert("20","12");
		bTree.insert("21","12");
		bTree.insert("22","12");
		bTree.insert("23","12");
		bTree.insert("11.5","12");
		String data = String.valueOf(bTree.search("10"));
		System.out.println(data);
		bTree.update("10","5,11");
		data = String.valueOf(bTree.search("10"));
		System.out.println(data);

		System.out.println(bTree.toString());
		try {
			LinkedList<Pointer> list = bTree.searchByRange("10", "17");
			for (int i = 0; i < list.size(); i++) {
				System.out.println(list.get(i).getKey() + ", " + list.get(i).getValue());
			}
		}catch (DBAppException e){
			e.printStackTrace();
		}

		serialize(bTree,"Student");
		BTree bTree1 = deserialize("Student");
		System.out.println(bTree1.toString());
	}
}
