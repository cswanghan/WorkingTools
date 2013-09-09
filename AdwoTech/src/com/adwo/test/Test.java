package com.adwo.test;

import com.adwo.Questionnaire.Questionnaire;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Questionnaire.GetPeriodAnswerFromMySql("2012-09-25", "2012-09-26");
		Questionnaire.GetPeriodUserAppActivityFromHive("2012-09-25", "2012-09-26");
	}

}
