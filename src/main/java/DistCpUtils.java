/**
 *
 */


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.StringUtils;

/**
 * @author yqi
 *
 */
public class DistCpUtils
{
	public static String bytesString(long b)
	{
      return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

	/** rename tmp to dst, delete dst if already exists */
	static public void rename(FileSystem destFileSys, Path tmp, Path dst) throws IOException {
		try {
			if (destFileSys.exists(dst)) {
				destFileSys.delete(dst, true);
			}
			if (!destFileSys.rename(tmp, dst)) {
				throw new IOException();
			}
		}
		catch(IOException cause) {
			throw (IOException)new IOException("Fail to rename tmp file (=" + tmp
					+ ") to destination file (=" + dst + ")").initCause(cause);
		}
	}

//	static public SequenceFile.Writer getSeqFileWriter(FileSystem dfs, Path p,
//			Class<? extends Writable> KeyClass, Class<? extends Writable> ValueClass)
//	throws IOException {
//
//		if (dfs.exists(p)) {
//			dfs.delete(p, false);
//		}
//
//		SequenceFile.Writer writer = SequenceFile.createWriter(dfs,
//				dfs.getConf(), p, KeyClass, ValueClass, 10000000,
//				(byte) 3, EVENT_BLOCK_SIZE, SequenceFile.CompressionType.BLOCK,
//				new GzipCodec(), new report(), new Metadata());
//
//		return writer;
//
//	}

//	static public SequenceFile.Writer getSeqFileWriter(FileSystem dfs, Path p)
//	throws IOException {
//
//		return getSeqFileWriter(dfs, p, ByteWritable.class, RawTuple.class);
//
//	}

	/** Check whether the file list have duplication. */
	static public void checkDuplication(FileSystem fs, Path file, Path sorted,
			Configuration conf) throws IOException {
		SequenceFile.Reader in = null;
		try {
			SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs,
					new Text.Comparator(), Text.class, Text.class, conf);
			sorter.sort(file, sorted);
			in = new SequenceFile.Reader(fs, sorted, conf);

			Text prevdst = null, curdst = new Text();
			Text prevsrc = null, cursrc = new Text();
			for(; in.next(curdst, cursrc); ) {
				if (prevdst != null && curdst.equals(prevdst)) {
					throw new DuplicationException(
							"Invalid input, there are duplicated files in the sources: "
							+ prevsrc + ", " + cursrc);
				}
				prevdst = curdst;
				curdst = new Text();
				prevsrc = cursrc;
				cursrc = new Text();
			}
		}
		finally {
			checkAndClose(in);
		}
	}

	//is x an ancestor path of y?
	static public boolean isAncestorPath(String x, String y) {
		if (!y.startsWith(x)) {
			return false;
		}
		final int len = x.length();
		return y.length() == len || y.charAt(len) == Path.SEPARATOR_CHAR;
	}

	static public boolean checkAndClose(java.io.Closeable io)
	{
		if (io != null) {
			try {
				io.close();
			}
			catch(IOException ioe) {
				DistCPPlus.LOG.warn(StringUtils.stringifyException(ioe));
				return false;
			}
		}
		return true;
	}

	/** Delete the dst files/dirs which do not exist in src */
	static public void deleteNonexisting(
			FileSystem dstfs, FileStatus dstroot, Path dstsorted,
			FileSystem jobfs, Path jobdir, JobConf jobconf, Configuration conf
	) throws IOException {
		if (!dstroot.isDir()) {
			throw new IOException("dst must be a directory when option "
					+ Options.DELETE.cmd + " is set, but dst (= " + dstroot.getPath()
					+ ") is not a directory.");
		}

		//write dst lsr results
		final Path dstlsr = new Path(jobdir, "_distcp_dst_lsr");
		final SequenceFile.Writer writer = SequenceFile.createWriter(jobfs, jobconf,
				dstlsr, Text.class, dstroot.getClass(),
				SequenceFile.CompressionType.NONE);
		try {
			//do lsr to get all file statuses in dstroot
			final Stack<FileStatus> lsrstack = new Stack<FileStatus>();
			for(lsrstack.push(dstroot); !lsrstack.isEmpty(); ) {
				final FileStatus status = lsrstack.pop();
				if (status.isDir()) {
					for(FileStatus child : dstfs.listStatus(status.getPath())) {
						String relative = DistCPPlus.makeRelative(dstroot.getPath(), child.getPath());
						writer.append(new Text(relative), child);
						lsrstack.push(child);
					}
				}
			}
		} finally {
			checkAndClose(writer);
		}

		//sort lsr results
		final Path sortedlsr = new Path(jobdir, "_distcp_dst_lsr_sorted");
		SequenceFile.Sorter sorter = new SequenceFile.Sorter(jobfs,
				new Text.Comparator(), Text.class, FileStatus.class, jobconf);
		sorter.sort(dstlsr, sortedlsr);

		//compare lsr list and dst list
		SequenceFile.Reader lsrin = null;
		SequenceFile.Reader dstin = null;
		try {
			lsrin = new SequenceFile.Reader(jobfs, sortedlsr, jobconf);
			dstin = new SequenceFile.Reader(jobfs, dstsorted, jobconf);

			//compare sorted lsr list and sorted dst list
			final Text lsrpath = new Text();
			final FileStatus lsrstatus = new FileStatus();
			final Text dstpath = new Text();
			final Text dstfrom = new Text();
			final FsShell shell = new FsShell(conf);

			final String[] shellargs = {"-rmr", null};

			boolean hasnext = dstin.next(dstpath, dstfrom);
			for(; lsrin.next(lsrpath, lsrstatus); ) {
				int dst_cmp_lsr = dstpath.compareTo(lsrpath);
				for(; hasnext && dst_cmp_lsr < 0; ) {
					hasnext = dstin.next(dstpath, dstfrom);
					dst_cmp_lsr = dstpath.compareTo(lsrpath);
				}

				if (dst_cmp_lsr == 0) {
					//lsrpath exists in dst, skip it
					hasnext = dstin.next(dstpath, dstfrom);
				}
				else {
					//lsrpath does not exist, delete it
					String s = new Path(dstroot.getPath(), lsrpath.toString()).toString();
					if (shellargs[1] == null || !isAncestorPath(shellargs[1], s)) {
						shellargs[1] = s;
						int r = 0;
						try {
							r = shell.run(shellargs);
						} catch(Exception e) {
							throw new IOException("Exception from shell.", e);
						}
						if (r != 0) {
							throw new IOException("\"" + shellargs[0] + " " + shellargs[1]);
						}
					}
				}
			}
		} finally {
			checkAndClose(lsrin);
			checkAndClose(dstin);
		}
	}
	
	/**
	 * Check whether the contents of src and dst are the same.
	 *
	 * Return false if dstpath does not exist
	 *
	 * YQ: If the files have different time stamps of last modification, return false.
	 *
	 * If the files have different sizes, return false.
	 *
	 * If the files have the same sizes, the file checksums will be compared.
	 *
	 * When file checksum is not supported in any of file systems,
	 * two files are considered as the same if they have the same size.
	 */
	static public boolean sameFile(FileSystem srcfs, FileStatus srcstatus,
			FileSystem dstfs, FileStatus dststatus, boolean skipCRCCheck,
			boolean skipTSCheck) throws IOException {
		
		if(!skipTSCheck && !sameTimeStamps(srcstatus, dststatus))
		{
			return false;
		}
		
		//    System.out.println("YAN - source size : "+srcstatus.getLen());
		//    System.out.println("YAN - target size : "+dststatus.getLen());

		//same length?
		if (srcstatus.getLen() != dststatus.getLen()) {
			return false;
		}

		//    System.out.println("YAN - skipCRCCheck? "+skipCRCCheck);
		if (skipCRCCheck) {
			DistCPPlus.LOG.debug("Skipping CRC Check");
			return true;
		}

		//get src checksum
		final FileChecksum srccs;
		try {
			srccs = srcfs.getFileChecksum(srcstatus.getPath());
		} catch(FileNotFoundException fnfe) {
			/*
			 * Two possible cases:
			 * (1) src existed once but was deleted between the time period that
			 *     srcstatus was obtained and the try block above.
			 * (2) srcfs does not support file checksum and (incorrectly) throws
			 *     FNFE, e.g. some previous versions of HftpFileSystem.
			 * For case (1), it is okay to return true since src was already deleted.
			 * For case (2), true should be returned.
			 */
			return true;
		}

		//compare checksums
		try {
			final FileChecksum dstcs = dstfs.getFileChecksum(dststatus.getPath());
			//return true if checksum is not supported
			//(i.e. some of the checksums is null)
			//      System.out.println("YAN - source checksum : "+srccs);
			//      System.out.println("YAN - source checksum : "+srccs);
			return srccs == null || dstcs == null || srccs.equals(dstcs);
		} catch(FileNotFoundException fnfe) {
			//    	System.out.println("YAN - False is going to return!!!");
			return false;
		}
	}

	/**
	 * Check whether the contents of src and dst are the same.
	 *
	 * Return false if dstpath does not exist
	 *
	 * YQ: If the files have different time stamps of last modification, return false.
	 *
	 * If the files have different sizes, return false.
	 *
	 * If the files have the same sizes, the file checksums will be compared.
	 *
	 * When file checksum is not supported in any of file systems,
	 * two files are considered as the same if they have the same size.
	 */
	static public boolean sameFile(FileSystem srcfs, FileStatus srcstatus,
			FileSystem dstfs, Path dstpath, boolean skipCRCCheck,
			boolean skipTSCheck) throws IOException {
		
		return sameFile(srcfs, srcstatus, dstfs, 
				dstfs.getFileStatus(dstpath), skipCRCCheck, skipTSCheck);
		
	}

	/**
	 * Check whether the contents of src and dst are the same.
	 *
	 * Return false if dstpath does not exist
	 *
	 * YQ: If the files have different time stamps of last modification, return false.
	 *
	 */
	static public boolean sameTimeStamps(FileStatus srcstatus,
			FileStatus dststatus) throws IOException
	{

		if(srcstatus.getModificationTime() == dststatus.getModificationTime())
		{
			return true;
		}

		return false;
	}
	
	/**
	 * Check whether the contents of src and dst are the same.
	 *
	 * Return false if dstpath does not exist
	 *
	 * YQ: If the files have different time stamps of last modification, return false.
	 *
	 *
	 */
	static public boolean sameTimeStamps(FileStatus srcstatus,
			FileSystem dstfs, Path dstpath) throws IOException
	{
		FileStatus dststatus;
		try {
			dststatus = dstfs.getFileStatus(dstpath);
		} catch(FileNotFoundException fnfe) {
			return false;
		}

		return sameTimeStamps(srcstatus, dststatus);
	}

	/** Sanity check for srcPath */
	public static void checkSrcPath(JobConf jobConf, List<Path> srcPaths)
	throws IOException {
		List<IOException> rslt = new ArrayList<IOException>();

		Path[] ps = new Path[srcPaths.size()];
		ps = srcPaths.toArray(ps);
		TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(), ps, jobConf);

		for (Path p : srcPaths) {
			FileSystem fs = p.getFileSystem(jobConf);
			if (!fs.exists(p)) {
				rslt.add(new IOException("Input source " + p + " does not exist."));
			}
		}
		if (!rslt.isEmpty()) {
			throw new InvalidInputException(rslt);
		}
	}

	public static List<Path> fetchFileList(Configuration conf, Path srcList)
	throws IOException {
		List<Path> result = new ArrayList<Path>();
		FileSystem fs = srcList.getFileSystem(conf);
		BufferedReader input = null;
		try {
			input = new BufferedReader(new InputStreamReader(fs.open(srcList)));
			String line = input.readLine();
			while (line != null) {
				result.add(new Path(line));
				line = input.readLine();
			}
		} finally {
			checkAndClose(input);
		}
		return result;
	}

	/** Fully delete dir */
	static void fullyDelete(String dir, Configuration conf) throws IOException {
		if (dir != null) {
			Path tmp = new Path(dir);
			if (!tmp.getFileSystem(conf).exists(tmp)) {
				DistCPPlus.LOG.warn("Path " + tmp + " does not exist! Could not be cleaned");
			} else {
				boolean success = tmp.getFileSystem(conf).delete(tmp, true);
				if (!success) {
					DistCPPlus.LOG.warn("Could not fully delete " + tmp);
				}
			}
		}
	}

}
