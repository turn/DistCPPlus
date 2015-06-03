/**
 * Copyright (C) 2012-2013 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */


import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

/**
 * FSCopyFilesMapper: The mapper for copying files between FileSystems.
 */
class DefaultCopyFilesMapper implements Mapper<LongWritable, FilePair, WritableComparable<?>, Text>
{
	
	// config
	protected int sizeBuf = 128 * 1024;
	protected FileSystem destFileSys = null;
	protected boolean ignoreReadFailures;
	protected boolean preserve_status;
	protected EnumSet<FileAttribute> preseved;
	protected boolean overwrite;
	protected boolean update;
	protected Path destPath = null;
	protected JobConf job;
	protected boolean skipCRCCheck = false;
	protected boolean skipTSCheck = false;

	// stats
	protected int failcount = 0;
	protected int skipcount = 0;
	protected int copycount = 0;

	protected String getCountString() {
		return "Copied: " + copycount + " Skipped: " + skipcount
		+ " Failed: " + failcount;
	}
	protected void updateStatus(Reporter reporter) {
		reporter.setStatus(getCountString());
	}

	/**
	 * Return true if dst should be replaced by src and the update flag is set.
	 * Right now, this merely checks that the src and dst len are not equal.
	 * This should be improved on once modification times, CRCs, etc. can
	 * be meaningful in this context.
	 * @throws IOException
	 */
	protected boolean needsUpdate(FileStatus srcstatus,
			FileSystem dstfs, Path dstpath) throws IOException {
		return update && !DistCpUtils.sameFile(srcstatus.getPath().getFileSystem(job),
				srcstatus, dstfs, dstpath, skipCRCCheck, skipTSCheck);
	}

	/**
	 * Will be moved into the right class
	 * @deprecated
	 * @param f
	 * @param reporter
	 * @param srcstat
	 * @return
	 * @throws IOException
	 */
	protected FSDataOutputStream create(Path f, Reporter reporter,
			FileStatus srcstat) throws IOException {
		if (destFileSys.exists(f)) {
			destFileSys.delete(f, false);
		}
		if (!preserve_status) {
			return destFileSys.create(f, true, sizeBuf, reporter);
		}

		FsPermission permission = preseved.contains(FileAttribute.PERMISSION)?
				srcstat.getPermission(): null;
				short replication = preseved.contains(FileAttribute.REPLICATION)?
						srcstat.getReplication(): destFileSys.getDefaultReplication();
						long blockSize = preseved.contains(FileAttribute.BLOCK_SIZE)?
								srcstat.getBlockSize(): destFileSys.getDefaultBlockSize();
								return destFileSys.create(f, permission, true, sizeBuf, replication,
										blockSize, reporter);
	}

	/**
	 * Copy a file to a destination.
	 * @param srcstat src path and metadata
	 * @param dstpath dst path
	 * @param reporter
	 */
	protected void copy(FileStatus srcstat, Path relativedst,
			OutputCollector<WritableComparable<?>, Text> outc, Reporter reporter)
	throws IOException {
		Path absdst = new Path(destPath, relativedst);
		int totfiles = job.getInt(DistCPPlus.SRC_COUNT_LABEL, -1);
		assert totfiles >= 0 : "Invalid file count " + totfiles;

		// if a directory, ensure created even if empty
		if (srcstat.isDir()) {
			if (destFileSys.exists(absdst)) {
				if (!destFileSys.getFileStatus(absdst).isDir()) {
					throw new IOException("Failed to mkdirs: " + absdst+" is a file.");
				}
			}
			else if (!destFileSys.mkdirs(absdst)) {
				throw new IOException("Failed to mkdirs " + absdst);
			}
			// TODO: when modification times can be set, directories should be
			// emitted to reducers so they might be preserved. Also, mkdirs does
			// not currently return an error when the directory already exists;
			// if this changes, all directory work might as well be done in reduce
			return;
		}

		if (destFileSys.exists(absdst) && !overwrite
				&& !needsUpdate(srcstat, destFileSys, absdst)) {
			outc.collect(null, new Text("SKIP: " + srcstat.getPath()));
			++skipcount;
			reporter.incrCounter(DistCPPlus.Counter.SKIP, 1);
			updateStatus(reporter);
			return;
		}

//		System.out.println("DEST PATH: "+job.get(DistCPPlus.TMP_DIR_LABEL));
//		System.out.println("DEST FILE: "+relativedst);
		Path tmpfile = new Path(job.get(DistCPPlus.TMP_DIR_LABEL), relativedst);
		byte[] buffer = new byte[sizeBuf];
		long cbcopied = 0L;
	      FSDataInputStream in = null;
	      FSDataOutputStream out = null;
	      try {
	        // open src file
	        in = srcstat.getPath().getFileSystem(job).open(srcstat.getPath());
	        reporter.incrCounter(DistCPPlus.Counter.BYTESEXPECTED, srcstat.getLen());
	        // open tmp file
	        out = create(tmpfile, reporter, srcstat);
	        // copy file
	        for(int cbread; (cbread = in.read(buffer)) >= 0; ) {
	          out.write(buffer, 0, cbread);
	          cbcopied += cbread;
	          reporter.setStatus(
	              String.format("%.2f ", cbcopied*100.0/srcstat.getLen())
	              + absdst + " [ " +
	              StringUtils.humanReadableInt(cbcopied) + " / " +
	              StringUtils.humanReadableInt(srcstat.getLen()) + " ]");
	        }
	      } finally {
	    	  DistCpUtils.checkAndClose(in);
	    	  DistCpUtils.checkAndClose(out);
	      }

	      if (cbcopied != srcstat.getLen()) {
	        throw new IOException("File size not matched: copied "
	            + DistCpUtils.bytesString(cbcopied) + " to tmpfile (=" + tmpfile
	            + ") but expected " + DistCpUtils.bytesString(srcstat.getLen())
	            + " from " + srcstat.getPath());
	      }
	      else {
	        if (totfiles == 1) {
	          // Copying a single file; use dst path provided by user as destination
	          // rather than destination directory, if a file
	          Path dstparent = absdst.getParent();
	          if (!(destFileSys.exists(dstparent) &&
	                destFileSys.getFileStatus(dstparent).isDir())) {
	            absdst = dstparent;
	          }
	        }
	        if (destFileSys.exists(absdst) &&
	            destFileSys.getFileStatus(absdst).isDir()) {
	          throw new IOException(absdst + " is a directory");
	        }
	        if (!destFileSys.mkdirs(absdst.getParent())) {
	          throw new IOException("Failed to create parent dir: " + absdst.getParent());
	        }
	        DistCpUtils.rename(destFileSys, tmpfile, absdst);

	        FileStatus dststat = destFileSys.getFileStatus(absdst);
	        if (dststat.getLen() != srcstat.getLen()) {
	          destFileSys.delete(absdst, false);
	          throw new IOException("File size not matched: copied "
	              + DistCpUtils.bytesString(dststat.getLen()) + " to dst (=" + absdst
	              + ") but expected " + DistCpUtils.bytesString(srcstat.getLen())
	              + " from " + srcstat.getPath());
	        }
	        updatePermissions(srcstat, dststat);
	      }
		// report at least once for each file
		++copycount;
		reporter.incrCounter(DistCPPlus.Counter.BYTESCOPIED, cbcopied);
		reporter.incrCounter(DistCPPlus.Counter.COPY, 1);
		updateStatus(reporter);
	}

	protected void updatePermissions(FileStatus src, FileStatus dst
	) throws IOException {
		if (preserve_status) {
			DistCPPlus.updatePermissions(src, dst, preseved, destFileSys);
		}
	}


	/** Mapper configuration.
	 * Extracts source and destination file system, as well as
	 * top-level paths on source and destination directories.
	 * Gets the named file systems, to be used later in map.
	 */
	public void configure(JobConf job)
	{
		destPath = new Path(job.get(DistCPPlus.DST_DIR_LABEL, "/"));
		try {
			destFileSys = destPath.getFileSystem(job);
		} catch (IOException ex) {
			throw new RuntimeException("Unable to get the named file system.", ex);
		}
		sizeBuf = job.getInt("copy.buf.size", 128 * 1024);
		ignoreReadFailures = job.getBoolean(Options.IGNORE_READ_FAILURES.propertyname, false);
		preserve_status = job.getBoolean(Options.PRESERVE_STATUS.propertyname, false);
		if (preserve_status) {
			preseved = FileAttribute.parse(job.get(DistCPPlus.PRESERVE_STATUS_LABEL));
		}
		update = job.getBoolean(Options.UPDATE.propertyname, false);
		overwrite = !update && job.getBoolean(Options.OVERWRITE.propertyname, false);
		skipCRCCheck = job.getBoolean(Options.SKIPCRC.propertyname, false);
		skipTSCheck = job.getBoolean(Options.SKIPTS.propertyname, false);
		this.job = job;
	}

	/** Map method. Copies one file from source file system to destination.
	 * @param key src len
	 * @param value FilePair (FileStatus src, Path dst)
	 * @param out Log of failed copies
	 * @param reporter
	 */
	public void map(LongWritable key, FilePair value,
			OutputCollector<WritableComparable<?>, Text> out,
			Reporter reporter) throws IOException {
		final FileStatus srcstat = value.input;
		final Path relativedst = new Path(value.output);
		HadoopHeartBeat heart_beat = new HadoopHeartBeat(); //FIXME do we need add this here, or copy(...)???
		try {
			heart_beat.startHeartbeat(reporter, 5000, 5000); // FIXME how does 5000 come from?
			copy(srcstat, relativedst, out, reporter);
		} catch (IOException e) {
			++failcount;
			reporter.incrCounter(DistCPPlus.Counter.FAIL, 1);
			updateStatus(reporter);
			final String sfailure = "FAIL " + relativedst + " : " +

			StringUtils.stringifyException(e);
			out.collect(null, new Text(sfailure));
			DistCPPlus.LOG.info(sfailure);
			try {
				for (int i = 0; i < 3; ++i) {
					try {
						final Path tmp = new Path(job.get(DistCPPlus.TMP_DIR_LABEL), relativedst);
						if (destFileSys.delete(tmp, true))
							break;
					} catch (IOException ex) {
						// ignore, we are just cleaning up
						DistCPPlus.LOG.debug("Ignoring cleanup exception", ex);
					}
					// update status, so we don't get timed out
					updateStatus(reporter);
					Thread.sleep(3 * 1000);
				}
			} catch (InterruptedException inte) {
				throw (IOException)new IOException().initCause(inte);
			}
		} finally {
			updateStatus(reporter);
			heart_beat.stopHeatbeat();
		}
	}

	public void close() throws IOException
	{
		if (0 == failcount || ignoreReadFailures) {
			return;
		}
		throw new IOException(getCountString());
	}
}
