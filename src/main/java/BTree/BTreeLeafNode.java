package BTree;

import Main.DBApp;

import java.util.Vector;

class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> {
	protected final static int LEAFORDER = DBApp.nodeOrder;
	private Object[] values;

	public BTreeLeafNode() {
		this.keys = new Object[LEAFORDER + 1];
		this.values = new Object[LEAFORDER + 1];
	}

	@SuppressWarnings("unchecked")
	public Vector<TValue> getValue(int index) {
		return (Vector<TValue>) this.values[index];
	}

	public void setValue(int index, Vector<TValue> value) {
		this.values[index] = value;
	}

	public void setValueInVector(int index, int valueIndex, TValue value) {
		((Vector<TValue>) this.values[index]).set(valueIndex, value);
	}

	public void addValueToVector(int index, TValue value) {
		((Vector<TValue>) this.values[index]).add(value);
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
