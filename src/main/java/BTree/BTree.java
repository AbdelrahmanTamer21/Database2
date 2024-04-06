package BTree;

import Main.Tuple;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different,
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements java.io.Serializable {
	private final String indexName;
	private final boolean allowDuplicates;
	private BTreeNode<TKey> root;

	public BTree(String indexName, boolean allowDuplicates) {
		this.root = new BTreeLeafNode<TKey, TValue>();
		this.indexName = indexName;
		this.allowDuplicates = allowDuplicates;
	}

	public String getIndexName() {
		return this.indexName;
	}

	public int getRootKeyCount(){
		return root.keyCount;
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		//Handle Duplicates
		if(search(key) != null && !allowDuplicates){
			return;
		}
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

	public void delete(TKey key, TValue value){
		BTreeLeafNode<TKey, TValue> leaf = this.getLeafNodeForMinVal(key);
		boolean flag = false;
		while(!flag && leaf != null){
			flag = leaf.delete(key,value);

			if (leaf.isUnderflow()) {
				BTreeNode<TKey> n = leaf.dealUnderflow();
				if (n != null)
					this.root = n;
			}
			leaf = leaf.getRightSibling();
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

	/**
	 * Get the range of values in this B+ tree that are between this min and max
	 */

	public LinkedList<Pointer<TKey,TValue>> searchByRange(TKey min, TKey max){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> minNode = this.getLeafNodeForMinVal(min);
		// Keep going to the right sibling and add all the values that fit the range into a linkedList
		do{
			for (int i = 0; i < minNode.getKeyCount(); i++) {
				if(minNode.getKey(i).compareTo(min) >= 0 && minNode.getKey(i).compareTo(max) <= 0){
					list.add(new Pointer<>(minNode.getKey(i),minNode.getValue(i)));
				}

				if(minNode.getKey(i).compareTo(min) < 0 && minNode.getKey(i).compareTo(max) >= 0){
					return list;
				}
			}
			minNode = minNode.getRightSibling();
		}while (minNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getLessThanKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(key) < 0){
					list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i)));
				}else {
					return list;
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getLessThanOrEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(key) <= 0){
					list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i)));
				}else {
					return list;
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getMoreThanKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeForMinVal(key);
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(key) == 0){
					continue;
				}
				if(currentNode.getKey(i).compareTo(key) > 0){
					list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i)));
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getMoreThanOrEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeForMinVal(key);
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(key) >= 0){
					list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i)));
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> getNotEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(key) != 0){
					list.add(new Pointer<>(currentNode.getKey(i),currentNode.getValue(i)));
				}
			}
			currentNode = currentNode.getRightSibling();
		}while (currentNode != null);
		return list;
	}

	public LinkedList<Pointer<TKey,TValue>> computeOperator(TKey key, String operator){
		return switch (operator) {
			case "<" -> getLessThanKeys(key);
			case "<=" -> getLessThanOrEqualKeys(key);
			case ">" -> getMoreThanKeys(key);
			case ">=" -> getMoreThanOrEqualKeys(key);
			case "!=" -> getNotEqualKeys(key);
			case "=" -> searchByRange(key, key);
			default -> new LinkedList<>();
		};
	}


	/**
	 * Find the leaf node that contains the specified key.
	 */
	private BTreeLeafNode<TKey, TValue> getLeafNodeForMinVal(TKey key) {
		BTreeNode<TKey> currentNode = this.root;

		while (currentNode instanceof BTreeInnerNode<TKey> innerNode) {
			int childIndex = innerNode.getChildIndex(key);

			if (childIndex == -1) {
				// Key is smaller than all children, follow the leftmost child
				currentNode = innerNode.getChild(0);
			} else {
				// Key is greater than or equal to the child at the specified index
				currentNode = innerNode.getChild(childIndex);
			}
		}
		return (BTreeLeafNode<TKey, TValue>) currentNode;
	}

	public BTreeLeafNode<TKey, TValue> getFirstLeafNodeOnLeft() {
		BTreeNode<TKey> currentNode = this.root;

		// Traverse towards the leftmost leaf node
		while (currentNode instanceof BTreeInnerNode<TKey>) {
			currentNode = ((BTreeInnerNode<TKey>) currentNode).getChild(0); // Follow the leftmost child
		}

		return (BTreeLeafNode<TKey, TValue>) currentNode;
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}

		return (BTreeLeafNode<TKey, TValue>)node;
	}

	public boolean checkKeyExists(TKey key){
		return search(key) != null;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreeNode<TKey>> queue = new LinkedList<>();
		queue.offer(root);

		while (!queue.isEmpty()) {
			int levelSize = queue.size();

			for (int i = 0; i < levelSize; i++) {
				BTreeNode<TKey> node = queue.poll();

				// Append node with a border
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
		String[] strings = sb.toString().split("\n");
		StringBuilder result = new StringBuilder();
		for(int i = 0; i<strings.length;i++){
			strings[i] = (" ").repeat((strings[strings.length-1].length()-strings[i].length())/2) + strings[i];
			strings[i] = "Level " + i + ": " + strings[i];
			result.append(strings[i]).append("\n");
		}

		return result.toString();
	}

	//Method to serialize the B+ tree

	public static void serialize(BTree bTree, String tableName, String columnName) {
		try {
			//you may also write this verbosely as
			// FileOutputStream = new FileOutputStream(fileName);
			FileOutputStream fileOutputStream = new FileOutputStream( "Indices/" + tableName + "/" + columnName + "Index.ser");

			ObjectOutputStream objOutputStream = new ObjectOutputStream(fileOutputStream);

			objOutputStream.writeObject(bTree);
			//we don't want a memory leak if we can avoid it
			fileOutputStream.close();
			objOutputStream.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// Method to deserialize the B+ tree
	public static <TKey extends Comparable<TKey>> BTree<TKey, String> deserialize(String tableName, String columnName){
		try {
			FileInputStream fileInputStream = new FileInputStream ("Indices/" + tableName + "/" + columnName + "Index.ser");

			ObjectInputStream objInputStream = new ObjectInputStream (fileInputStream);

			BTree<TKey, String> bTree = (BTree<TKey, String>) objInputStream.readObject();

			objInputStream.close();
			fileInputStream.close();

			return bTree;

		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args){
		BTree bTree = new BTree("gpaIndex",true);
		//123 is the value in the table while 5,12 is the pointer to the 5th page in the 12th index
		//insert(key,value)

		bTree.insert("10","5,12");
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

		System.out.println(bTree);
		LinkedList<Pointer> list = bTree.searchByRange("1", "14");
		for (Pointer pointer : list) {
			System.out.println(pointer.getKey() + ", " + pointer.getValue());
		}

		bTree.delete("13");
		System.out.println(bTree);


		serialize(bTree,"Student","gpa");
		BTree bTree1 = deserialize("Student","gpa");
		System.out.println(bTree1.indexName);
		System.out.println(bTree1);
	}
}
