package mx.j2.recommend.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FileTool {

	/**
	 * 给定文件，读取全部内容
	 * 
	 * @param fileName
	 * @return String
	 */
	public static String readContent(String fileName) {
		File file = new File(fileName);
//		String result = "";
		StringBuilder resultSb = new StringBuilder();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				resultSb.append(tempString);
//				result = result + tempString;
			}
			reader.close();
			return resultSb.toString();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
		return "";
	}
}
