package com.turn.hadoop.distcp;


import java.io.IOException;

/** An exception class for duplicated source files. */
public class DuplicationException extends IOException
{
	private static final long serialVersionUID = 1L;
	/** Error code for this exception */
	public static final int ERROR_CODE = -2;
	DuplicationException(String message) {super(message);}
}
