package com.turn.hadoop.distcp;
/** Copyright (C) 2013 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class FilePair implements Writable
{
	FileStatus input = new FileStatus();
	String output;
	public FilePair() { }
	FilePair(FileStatus input, String output) {
		this.input = input;
		this.output = output;
	}

	public void readFields(DataInput in) throws IOException {
		input.readFields(in);
		output = Text.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		input.write(out);
		Text.writeString(out, output);
	}

	public String toString() {
		return input + " : " + output;
	}
	
	public FileStatus getInput() {
		return input;
	}
	
	public String getOutput() {
		return output;
	}
	
}
