/**
 * 
 */
package com.adwo.algorithmpackage;

/**
 * @author dev
 * 
 */
public class ReverseLinkedListRecursively {

	public static void main(String args[]) {
		ReverseLinkedListRecursively reverser = new ReverseLinkedListRecursively();
		SingleLinkedList<Integer> originalList = reverser
				.getLabRatList(3);
		System.out.println("Original List : " + originalList.toString());
		originalList.start = reverser.reverse(originalList.start);
		System.out.println("Reversed List : " + originalList.toString());
	}

	public Node<Integer> reverse(Node<Integer> head) {
		if (head == null || head.next == null)
			return head;
		Node<Integer> nextItem = head.next;
		head.next = null;
		Node<Integer> reverseRest = reverse(nextItem);
		nextItem.next = head;
		return reverseRest;
	}

	private SingleLinkedList<Integer> getLabRatList(int count) {
		SingleLinkedList<Integer> sampleList = new SingleLinkedList<Integer>();
		for (int i = 0; i < count; i++) {
			sampleList.add(i);
		}
		return sampleList;
	}
}
/*
 * SAMPLE OUTPUT Original List : 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 Reversed List : 9,
 * 8, 7, 6, 5, 4, 3, 2, 1, 0
 */