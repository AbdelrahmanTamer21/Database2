package BTree;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different,
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements java.io.Serializable {
	private final String indexName;
	private final String colName;
	private BTreeNode<TKey> root;

	public BTree(String indexName, String colName) {
		this.root = new BTreeLeafNode<TKey, TValue>();
		this.indexName = indexName;
		this.colName = colName;
	}

	public String getColName() {
		return colName;
	}
	public int getRootKeyCount(){
		return root.keyCount;
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		//Handle Duplicates
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		if (search(key) != null) {
			leaf.insertKey(key, value);
			return;
		}
		leaf.insertKey(key, value);
		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n;
		}
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

			if (flag && leaf.isUnderflow()) {
				BTreeNode<TKey> n = leaf.dealUnderflow();
				if (n != null)
					this.root = n;
			}
			leaf = leaf.getRightSibling();
		}
	}

	public void deleteAll(){
		this.root = new BTreeLeafNode<TKey,TValue>();
	}


	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public Vector<TValue> search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}

	/**
	 * Get the range of values in this B+ tree that are between this min and max
	 */
	public LinkedList<Pointer<TKey,TValue>> getLessThanKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		BTreeLeafNode<TKey, TValue> currentNode = getFirstLeafNodeOnLeft();
		do{
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(key) < 0) {
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i), currentNode.getValue(i).get(j)));
					}
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
				if(currentNode.getKey(i).compareTo(key) <= 0) {
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i), currentNode.getValue(i).get(j)));
					}
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
				if(currentNode.getKey(i).compareTo(key) > 0) {
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i), currentNode.getValue(i).get(j)));
					}
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
				if(currentNode.getKey(i).compareTo(key) >= 0) {
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i), currentNode.getValue(i).get(j)));
					}
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
				if(currentNode.getKey(i).compareTo(key) != 0) {
					for (int j = 0; j < currentNode.getValue(i).size(); j++) {
						list.add(new Pointer<>(currentNode.getKey(i), currentNode.getValue(i).get(j)));
					}
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
			case "=" -> getEqualKeys(key);
			default -> new LinkedList<>();
		};
	}

	public LinkedList<Pointer<TKey,TValue>> getEqualKeys(TKey key){
		LinkedList<Pointer<TKey,TValue>> list = new LinkedList<>();
		Vector<TValue> vector = search(key);
		for (TValue value : vector) {
			list.add(new Pointer<>(key, value));
		}
		return list;
	}

	public int getPageNumberForInsert(TKey primaryKey){
		BTreeLeafNode<TKey, TValue> currentNode = getLeafNodeBeforeKey(primaryKey);
		int pageNumber = 0;
		if(currentNode != null && currentNode.getRightSibling() == null && currentNode.getKey(currentNode.getKeyCount()-1).compareTo(primaryKey)<0) {
			return Integer.parseInt(((Vector<String>) currentNode.getValue(currentNode.getKeyCount() - 1)).get(0).split("-")[0]);
		}
		while (currentNode!=null){
			for (int i = 0; i < currentNode.getKeyCount(); i++) {
				if(currentNode.getKey(i).compareTo(primaryKey) < 0){
					pageNumber = Integer.parseInt(((Vector<String>) currentNode.getValue(i)).get(0).split("-")[0]);
				} else if(currentNode.getKey(i).compareTo(primaryKey) == 0){
					return -1;
				}else {
					return pageNumber;
				}
			}
			currentNode = currentNode.getRightSibling();
		}
		return 1;
	}

	private BTreeLeafNode<TKey, TValue> getLeafNodeBeforeKey(TKey key) {
		BTreeNode<TKey> currentNode = this.root;
		BTreeLeafNode<TKey, TValue> prevLeafNode = null;

		while (currentNode instanceof BTreeInnerNode<TKey> innerNode) {
			int childIndex = innerNode.getChildIndex(key);
			if (childIndex == -1) {
				// Key is smaller than all children, follow the leftmost child
				currentNode = innerNode.getChild(0);
			} else {
				// Key is greater than or equal to the child at the specified index
				currentNode = innerNode.getChild(childIndex+1);
				if (!(currentNode instanceof BTreeInnerNode)) {
					prevLeafNode = (BTreeLeafNode<TKey, TValue>) currentNode;
				}
			}
		}
		return prevLeafNode;
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
				assert node != null;
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

	public String toStringRangeValues() {
		StringBuilder sb = new StringBuilder();
		Queue<BTreeNode<TKey>> queue = new LinkedList<>();
		queue.offer(root);

		while (!queue.isEmpty()) {
			int levelSize = queue.size();

			for (int i = 0; i < levelSize; i++) {
				BTreeNode<TKey> node = queue.poll();

				// Append node with a border
				assert node != null;
				sb.append("|").append(node.toStringRange().trim());

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
}
