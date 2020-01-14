package com.compare;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
/**
 * Object storing the  different
 * @author Jay Sangoi
 *
 */
public class Diff implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4149820978601050602L;

	/**
	 * Key values
	 */
	String key;

	/**
	 * Columns whose values are different
	 */
	List<String> differentColumns = new ArrayList<>();

	/**
	 * Old values of columns which are different
	 */
	List<String> oldValues = new ArrayList<>();

	/**
	 * New values of columns which are different
	 */
	List<String> newValues = new ArrayList<>();

	@Override
	public String toString() {

		StringBuilder str = new StringBuilder("[");

		for (int i = 0; i < differentColumns.size(); i++) {
			
			if(i != 0) {
				str.append(",");
			}
			
			str.append(differentColumns.get(i)).append(",").append(oldValues.get(i)).append(",")
					.append(newValues.get(i));
			
		}

		str.append("]");

		return str.toString();
	}

}
