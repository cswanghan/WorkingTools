/**
 * 
 */
package com.adwo.algorithmpackage;

import java.util.Stack;

/**
 * @author dev
 * 
 */
public class LadderCount {
	
	public static void main(String argv[]) {

		getMethodCount(new Stack<Integer>(), 5, 3);
	}

	public static void getMethodCount(Stack<Integer> stack, int max, int limit) 
	{
		for (int j = 1; j <= limit; j++) 
		{
			stack.push(j);
			if (max == j) 
			{
				System.out.println(stack);
				stack.pop();
				break;
			} else {
				getMethodCount(stack, max - j, limit);
				stack.pop();
			}
		}
	}
}
