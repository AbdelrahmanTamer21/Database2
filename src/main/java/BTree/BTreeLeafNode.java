package BTree;

import Main.DBApp;

import java.util.Vector;

class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> {
	protected final static int LEAFORDER = DBApp.nodeOrder;
	private final Vector<TValue>[] values;

	public BTreeLeafNode() {
		this.keys = new Object[LEAFORDER + 1];
		this.values = new Vector[LEAFORDER + 1];
	}

	public Vector<TValue> getValue(int index) {
		return this.values[index];
	}

	public void setValue(int index, Vector<TValue> value) {
		this.values[index] = value;
	}

	public void addValueToVector(int index, TValue value) {
		this.values[index].add(findIndexToInsertIn((String) value,index),value);
	}

	public BTreeLeafNode<TKey, TValue> getRightSibling() {
		return (BTreeLeafNode<TKey, TValue>) this.rightSibling;
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}

	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			 int cmp = this.getKey(i).compareTo(key);
			 if (cmp == 0) {
				 return i;
			 }
			 else if (cmp > 0) {
				 return -1;
			 }
		}

		return -1;
	}

	public int search(TKey key, TValue value) {
		int low = 0;
		int high = this.getKeyCount() - 1;

		while (low <= high) {
			int mid = low + (high - low) / 2;
			int cmp = this.getKey(mid).compareTo(key);

			if (cmp == 0 && this.getValue(mid).contains(value)) {
				return mid; // Key found
			} else if (cmp < 0) {
				low = mid + 1; // Search in the right half
			} else {
				high = mid - 1; // Search in the left half
			}
		}

		return -1; // Key not found
	}


	/* The codes below are used to support insertion operation */

	public void insertKey(TKey key, TValue value) {
		int i = search(key);
		if (i != -1) {
			addValueToVector(i, value);
			return;
		}
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;
		Vector<TValue> v = new Vector<>();
		v.add(value);
		this.insertAt(index, key, v);
	}

	public void insertKeyVector(TKey key, Vector<TValue> value) {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;
		this.insertAt(index, key, value);
	}

	private void insertAt(int index, TKey key, Vector<TValue> value) {
		// move space for the new key
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setValue(i + 1, this.getValue(i));
		}

		// insert new key and value
		this.setKey(index, key);
		this.setValue(index, value);
		++this.keyCount;
	}

	public int findIndexToInsertIn(String newString, int index) {
		int left = 0;
		int right = values[index].size() - 1;

		while (left <= right) {
			int mid = left + (right - left) / 2;
			String midString = (String) values[index].get(mid);
			String[] parts = midString.split("-");
			int pageNum = Integer.parseInt(parts[0]);
			Object primaryKey = parseObject(parts[1]);

			String[] newParts = newString.split("-");
			int newPageNum = Integer.parseInt(newParts[0]);
			int newPrimaryKey = Integer.parseInt(newParts[1]);

			if (pageNum < newPageNum || (pageNum == newPageNum && compareTwoValues(primaryKey, newPrimaryKey, primaryKey.getClass().getName()) < 0)) {
				left = mid + 1;
			} else if (pageNum > newPageNum || compareTwoValues(primaryKey, newPrimaryKey, primaryKey.getClass().getName()) > 0) {
				right = mid - 1;
			} else {
				// If there are duplicate entries with the same pageNum and primaryKey,
				// we insert after the last occurrence of such entry.
				left = mid + 1;
			}
		}

		// Insert the newString at index 'left'
		return left;
	}

	private Object parseObject(String value){
		try {
			// Check if the value is an integer
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			try {
				// Check if the value is a double
				return Double.parseDouble(value);
			} catch (NumberFormatException e2) {
				// Return the value as a string
				return value;
			}
		}
	}

	private int compareTwoValues(Object primaryKey1, Object primaryKey2, String type) {
		return switch (type) {
			case "java.lang.Integer" -> Integer.compare((Integer) primaryKey1, (int) primaryKey2);
			case "java.lang.String" -> (String.valueOf(primaryKey1).compareTo((String) primaryKey2));
			case "java.lang.Double" -> Double.compare((Double) primaryKey1, (double) primaryKey2);
			default -> throw new IllegalStateException("Unexpected value: " + primaryKey2.getClass().getSimpleName());
		};
	}



	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to the parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		int midIndex = this.getKeyCount() / 2;

		BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));
			newRNode.setValue(i - midIndex, this.getValue(i));
			this.setKey(i, null);
			this.setValue(i, null);
		}
		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;

		return newRNode;
	}

	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}




	/* The codes below are used to support deletion operation */

	public boolean delete(TKey key) {
		int index = this.search(key);
		if (index == -1)
			return false;

		this.deleteAt(index);
		return true;
	}

	public boolean delete(TKey key, TValue value) {
		int index = this.search(key, value);
		if (index == -1)
			return false;
		if (this.getValue(index).size() > 1) {
			this.getValue(index).remove(value);
			return true;
		}
		this.deleteAt(index);
		return true;
	}

	private void deleteAt(int index) {
		int i;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setValue(i, this.getValue(i + 1));
		}
		this.setKey(i, null);
		this.setValue(i, null);
		--this.keyCount;
	}

	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Notice that the key sunk from parent is abandoned.
	 */
	@Override
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
		BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>)rightSibling;

		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));
			this.setValue(j + i, siblingLeaf.getValue(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();

		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}

	@Override
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>)sibling;

		this.insertKeyVector(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
		siblingNode.deleteAt(borrowIndex);

		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < this.getKeyCount(); ++i)
			s.append("(").append(this.getKey(i)).append(", ").append(this.getValue(i)).append(") ");
		return s.toString();
	}
}
