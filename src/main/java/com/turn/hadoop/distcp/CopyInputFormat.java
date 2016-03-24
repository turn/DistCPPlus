package com.turn.hadoop.distcp;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * InputFormat of a distcp job responsible for generating splits of the src
 * file list.
 */
class CopyInputFormat implements InputFormat<Text, Text>
{
	/**
	 * Produce splits such that each is no greater than the quotient of the
	 * total size and the number of splits requested.
	 * @param job The handle to the JobConf object
	 * @param numSplits Number of splits requested
	 */
	public InputSplit[] getSplits(JobConf job, int numSplits)
	throws IOException {

		int cnfiles = job.getInt(DistCPPlus.SRC_COUNT_LABEL, -1);
		long cbsize = job.getLong(DistCPPlus.TOTAL_SIZE_LABEL, -1);
		String srcfilelist = job.get(DistCPPlus.SRC_LIST_LABEL, "");
		if (cnfiles < 0 || cbsize < 0 || "".equals(srcfilelist)) {
			throw new RuntimeException("Invalid metadata: #files(" + cnfiles +
					") total_size(" + cbsize + ") listuri(" +
					srcfilelist + ")");
		}
		Path src = new Path(srcfilelist);
		FileSystem fs = src.getFileSystem(job);
		FileStatus srcst = fs.getFileStatus(src);

		ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
		LongWritable key = new LongWritable();
		FilePair value = new FilePair();
		final long targetsize = cbsize / numSplits;
		long pos = 0L;
		long last = 0L;
		long acc = 0L;
		long cbrem = srcst.getLen();
		SequenceFile.Reader sl = null;
		try {
			sl = new SequenceFile.Reader(fs, src, job);
			for (; sl.next(key, value); last = sl.getPosition()) {
				// if adding this split would put this split past the target size,
				// cut the last split and put this next file in the next split.
				if (acc + key.get() > targetsize && acc != 0) {
					long splitsize = last - pos;
					splits.add(new FileSplit(src, pos, splitsize, (String[])null));
					cbrem -= splitsize;
					pos = last;
					acc = 0L;
				}
				acc += key.get();
			}
		}
		finally {
			DistCpUtils.checkAndClose(sl);
		}
		if (cbrem != 0) {
			splits.add(new FileSplit(src, pos, cbrem, (String[])null));
		}
		return splits.toArray(new FileSplit[splits.size()]);
	}

	/**
	 * Returns a reader for this split of the src file list.
	 */
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException
	{
		return new SequenceFileRecordReader<Text, Text>(job, (FileSplit)split);
	}
}