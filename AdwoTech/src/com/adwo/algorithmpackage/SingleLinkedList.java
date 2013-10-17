/**
 * 
 */
package com.adwo.algorithmpackage;

/**
 * @author dev
 * 
 */
public class SingleLinkedList<E> {

	Node<E> start;
	int size;

	public SingleLinkedList() {
		start = null;
		size = 0;
	}

	// insertAtLast
	public void add(E data) {
		insertAtLast(data);
	}

	public void insertAtLast(E data) {
		if (size == 0) {
			start = new Node<E>();
			start.next = null;
			start.data = data;
		} else {
			Node<E> currentNode = getNodeAt(size - 1);
			Node<E> newNode = new Node<E>();
			newNode.data = data;
			newNode.next = null;
			currentNode.next = newNode;
		}
		size++;
	}

	public void insertAtFirst(E data) {
		if (size == 0) {
			start = new Node<E>();
			start.next = null;
			start.data = data;
		} else {
			Node<E> newNode = new Node<E>();
			newNode.data = data;
			newNode.next = start;
			start = newNode;
		}
		size++;
	}

	public Node<E> getNodeAt(int nodePos)
			throws ArrayIndexOutOfBoundsException {
		if (nodePos >= size || nodePos < 0) {
			throw new ArrayIndexOutOfBoundsException();
		}
		Node<E> temp = start;// Move pointer to front
		int counter = 0;
		for (; counter < nodePos; counter++) {
			temp = temp.next;
		}
		return temp;
	}

	public void insertAt(int position, E data) {
		if (position == 0) {
			insertAtFirst(data);
		} else if (position == size - 1) {
			insertAtLast(data);
		} else {
			Node<E> tempNode = getNodeAt(position - 1);
			Node<E> newNode = new Node<E>();
			newNode.data = data;
			newNode.next = tempNode.next;
			tempNode.next = newNode;
			size++;
		}
	}

	public Node<E> getFirst() {
		return getNodeAt(0);
	}

	public Node<E> getLast() {
		return getNodeAt(size - 1);
	}

	public E removeAtFirst() {
		if (size == 0) {
			throw new ArrayIndexOutOfBoundsException();
		}
		E data = start.data;
		start = start.next;
		size--;
		return data;
	}

	public E removeAtLast() {
		if (size == 0) {
			throw new ArrayIndexOutOfBoundsException();
		}
		Node<E> tempNode = getNodeAt(size - 2);
		E data = tempNode.next.data;
		tempNode.next = null;
		size--;
		return data;
	}

	public E removeAt(int position) {
		if (position == 0) {
			return removeAtFirst();
		} else if (position == size - 1) {
			return removeAtLast();
		} else {
			Node<E> tempNode = getNodeAt(position - 1);
			E data = tempNode.next.data;
			tempNode.next = tempNode.next.next;
			size--;
			return data;
		}
	}

	public int size() {
		return size;
	}

	public String toString() {
		if (size == 0) {
			return "";
		} else {
			StringBuilder output = new StringBuilder();
			Node<E> tempNode = start;
			while (tempNode.next != null) {
				output.append(tempNode.data).append(", ");
				tempNode = tempNode.next;
			}
			output.append(tempNode.data);
			return output.toString();
		}
	}

}