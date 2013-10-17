/**
 * 
 */
package com.adwo.algorithmpackage;

import java.util.Stack;

/**
 * @author dev
 *
 */
public class MatchedParentheses {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MatchedParentheses mp = new MatchedParentheses();
		if(mp.stackMatch("{{{{}}}") == 0)
			System.out.println("Wow, it matches!");
		else
			System.out.println("dude, you are wrong!");
	}

	/*
	 * 
	 */
	public int intMatch(String input_str)
	{
		int total = 0; 
		String[] details = input_str.split("");
		for(int i = 1; i < details.length; i++)
		{
			if(details[i].equals("{") || details[i] == "{")
				total++;
			else total--;
			System.out.println(details[i] + ":" + total);
		}
		return total;
	}
	
	public int stackMatch(String input_str)
	{
		Stack<String> stack = new Stack<String>();
		String[] details = input_str.split("");
		for(int i = 1; i < details.length; i++)
		{
			if(details[i].equals("{") || details[i] == "{")
				stack.push(details[i]);
			else stack.pop();
			System.out.println(stack.size());
		}
		return stack.size();
	}
}
