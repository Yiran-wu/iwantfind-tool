package com.iwantfind.utils;



public class Utils {
	public enum PrintType {
		INFO,
		ERROR,
		WARN
	}
	public static void print(PrintType type,  String msg) {
		if (type.equals(PrintType.ERROR))
			System.out.println("\033[31m [ERROR]" +  msg + "\033[0m");
		else if (type.equals(PrintType.INFO)) {
			System.out.println("\033[32m [ INFO]" + msg + "\033[0m");
		} else {
			System.out.println("\033[33m [ WARN]" + msg + "\033[0m");
		}
	}
	
}
