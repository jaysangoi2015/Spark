package com.rdu.compare;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * This class handles the array field. It will first create List<String> from List<Object> then it will sort it.
 * @author Jay Sangoi
 *
 */
public class HandleArrayField {

	public static String handle(String txt) {
		
		StringBuilder newTxt = new StringBuilder();
		
		Pattern pattern = Pattern.compile("(WrappedArray\\()(.*?)(\\))");
		
		Matcher m = pattern.matcher(txt);
		
		int lastIndex = 0;
		
		while(m.find()) {
			newTxt.append(txt.substring(lastIndex, m.start()));
			String temp = m.group(2);
			
			String ts[] = temp.split("],");
			
			List<String> colList = new ArrayList<>();
			
			for(String t:ts) {
				colList.add(t.replace(",", "_"));
			}
			
			Collections.sort(colList);
			for(String col :colList) {
				newTxt.append(col).append("],");
			}
			
			lastIndex = m.end();
			
		}

		newTxt.append(txt.substring(lastIndex));
		
		return newTxt.toString();
		
	}
	
	
	
}
