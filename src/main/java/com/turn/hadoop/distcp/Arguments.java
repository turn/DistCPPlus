package com.turn.hadoop.distcp;
/**
 * Copyright (C) 2013 Turn Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Maps;
/**
 * Class that parses and holds arguments for a distcp++ job.
 * @author drubin
 * @author yqi
 */
class Arguments {
		
	private static final String REGEX_PATH_SPLITTER = "/";
	
	final List<Path> srcs;
	final Path dst;
	final Path log;
	final EnumSet<Options> flags;
	final String preservedAttributes;
	final long filelimit;
	final long sizelimit;
	final String mapredSslConf;
	
	final boolean extendedRegex;
	final Path regexRoot;
	final boolean exportJob;
	final Set<FileStatus> regexTouchedDirectories;
	
	//cache file status when regexPath argument 
	//is used for distcpplus
	private Map<String, FileStatus> fsCache = null;

	/**
	 * Return the cache of file status map we build in 
	 * valueOf() when regexPath option is used
	 * 
	 * @return the cache map of <fileURI, FileStatus> 
	 */
	public Map<String, FileStatus> getFileStatusCache(){
		return fsCache;
	}

	/**
	 * Clean the cache to release memory.
	 */
	public void clearFileStatusCache(){
		
		if (fsCache != null) {
			fsCache.clear();
			fsCache = null;
		}
	}
	
	/**
	 * Arguments for distcp
	 * @param srcs List of source paths
	 * @param dst Destination path
	 * @param log Log output directory
	 * @param flags Command-line flags
	 * @param preservedAttributes Preserved attributes
	 * @param filelimit File limit
	 * @param sizelimit Size limit
	 * @param cache file status cache
	 */
	Arguments(List<Path> srcs, Path dst, Path log,
			EnumSet<Options> flags, String preservedAttributes,
			long filelimit, long sizelimit, String mapredSslConf,
			boolean extendedRegexMode, Path regexRoot, boolean exportJob,
			Set<FileStatus> regexTouchedDirectories, Map<String, FileStatus> cache) {
		this.srcs = srcs;
		this.dst = dst;
		this.log = log;
		this.flags = flags;
		this.preservedAttributes = preservedAttributes;
		this.filelimit = filelimit;
		this.sizelimit = sizelimit;
		this.mapredSslConf = mapredSslConf;
		
		this.extendedRegex = extendedRegexMode;
		this.regexRoot = regexRoot;
		this.exportJob = exportJob;
		this.regexTouchedDirectories = regexTouchedDirectories;
		if (DistCPPlus.LOG.isTraceEnabled()) {
			DistCPPlus.LOG.trace("this = " + this);
		}
		this.fsCache = cache;
	}

	static Arguments valueOf(String[] args, Configuration conf) throws IOException {
		List<Path> srcs = new ArrayList<Path>();
		Set<FileStatus> touchedDirs = new HashSet<FileStatus>();
		Set<Path> touchedDirsPathSet = new HashSet<Path>();
		Path dst = null;
		Path log = null;
		EnumSet<Options> flags = EnumSet.noneOf(Options.class);
		String presevedAttributes = null;
		String mapredSslConf = null;
		long filelimit = Long.MAX_VALUE;
		long sizelimit = Long.MAX_VALUE;
		boolean extendedRegex = false;
		boolean exportOnly = false;
		Path regexRoot = null;
		Map<String, FileStatus> fsCache = Maps.newHashMap();
		
		//one pass of all args to see if regexPath option is used
		for (int idx = 0; idx < args.length; idx++) {
			if ("-regexPath".equals(args[idx])){ 
			  extendedRegex = true;
			  break;
			}
		}
		
		for (int idx = 0; idx < args.length; idx++) {
			Options[] opt = Options.values();
			int i = 0;
			for(; i < opt.length && !args[idx].startsWith(opt[i].cmd); i++);

			if (i < opt.length) {
				flags.add(opt[i]);
				if (opt[i] == Options.PRESERVE_STATUS) {
					presevedAttributes =  args[idx].substring(2);
					FileAttribute.parse(presevedAttributes); //validation
				} else if (opt[i] == Options.FILE_LIMIT) {
					filelimit = Options.FILE_LIMIT.parseLong(args, ++idx);
				} else if (opt[i] == Options.SIZE_LIMIT) {
					sizelimit = Options.SIZE_LIMIT.parseLong(args, ++idx);
				}
			} else if ("-f".equals(args[idx])) {
				idx = incrementAndCheckNotLastArg(idx, args, "urilist_uri not specified in -f");
				srcs.addAll(DistCpUtils.fetchFileList(conf, new Path(args[idx])));
			} else if ("-log".equals(args[idx])) {
				idx = incrementAndCheckNotLastArg(idx, args, "logdir not specified in -log");
				log = new Path(args[idx]);
			} else if ("-mapredSslConf".equals(args[idx])) {
				idx = incrementAndCheckNotLastArg(idx, args, "ssl conf file not specified in -mapredSslConf");
				mapredSslConf = args[idx];
			} else if ("-m".equals(args[idx])) {
				idx = incrementAndCheckNotLastArg(idx, args, "num_maps not specified in -m");
				try {
					conf.setInt(DistCPPlus.MAX_MAPS_LABEL, Integer.valueOf(args[idx]));
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Invalid argument to -m: " +
							args[idx]);
				}
			} else if ("-mapper".equals(args[idx])) {

				//if a regular expression is provided for the source
				idx = incrementAndCheckNotLastArg(idx, args, "The full class name of the mapper class is required!");
				conf.set(DistCPPlus.COPY_MAPPER_CLASS_NAME_LABLE, args[idx]);

			} else if ("-market".equals(args[idx])) {

				//if a regular expression is provided for the source
				idx = incrementAndCheckNotLastArg(idx, args, "The market ID is expected!");
				try {
					conf.setInt(DistCPPlus.MARKET_ID_LABEL, Integer.valueOf(args[idx]));
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Invalid argument to -market: " +
							args[idx]);
				}

			} else if ("-rg".equals(args[idx])) {
				
				//if a regular expression is provided for the source
				idx = incrementAndCheckNotLastArg(idx, args, "A regular expression is expected for the source");
				String source_reg = args[idx];
				//1. decompose the input to get remote hdfs url, path and regular expressions
				String remote_hdfs_url = source_reg.subSequence(0, source_reg.indexOf('/', 7)).toString();
				String source_dir = source_reg.subSequence(source_reg.indexOf('/', 7), source_reg.lastIndexOf('/')).toString();
				String pattern = source_reg.substring(source_reg.lastIndexOf('/')+1);

				Collection<Path> path_collection = getFilePaths(
						FileSystem.get(URI.create(remote_hdfs_url), new Configuration()),
						source_dir, pattern, "");
				for (Path cur_src : path_collection)
				{
					srcs.add(cur_src);
				}
				
			} else if ("-regexPath".equals(args[idx])) {

				// Get source root folder
				idx = incrementAndCheckNotLastArg(idx, args, "A root folder is expected");
				String root = args[idx];
				regexRoot = new Path(args[idx]);
				
				// Get regex expression for child directories
				idx = incrementAndCheckNotLastArg(idx, args, "A regular expression is expected for the source");
				extendedRegex = true;
				String pattrn = args[idx];
				
				// Get host of remote source
				URI remoteHostURI = null;
				try {
					remoteHostURI = new URI(root);
				} catch (URISyntaxException e1) {
					throw new IllegalArgumentException("Could not parse regex root URI!", e1);
				}
				
				// Get root file
				FileSystem fs = FileSystem.get(remoteHostURI, new Configuration());
				findPaths(root, pattrn, fs, srcs, touchedDirs, fsCache);
				
			} else if ("-exportOnly".equals(args[idx])) {
				exportOnly = true;
			} else if ('-' == args[idx].codePointAt(0)) {
				throw new IllegalArgumentException("Invalid switch " + args[idx]);
			} else if (idx == args.length -1) {
				dst = new Path(args[idx]);
			} else {
				//disallow other source file when regexPath is used 
				if (extendedRegex){
					String info = "Invalid argument combination for regexPath use: \n";
					info += " detected both regexPath and seperate source files";
					throw new IllegalArgumentException(info);
				}
				srcs.add(new Path(args[idx]));
			}
		}
		// mandatory command-line parameters
		if (srcs.isEmpty() || dst == null) {
			throw new IllegalArgumentException("Missing "
					+ (dst == null ? "dst path" : "src"));
		}
		// incompatible command-line flags
		final boolean isOverwrite = flags.contains(Options.OVERWRITE);
		final boolean isUpdate = flags.contains(Options.UPDATE);
		final boolean isDelete = flags.contains(Options.DELETE);
		final boolean skipCRC = flags.contains(Options.SKIPCRC);
		final boolean skipTS = flags.contains(Options.SKIPTS);
		final boolean skipUpdateCheck = flags.contains(Options.SKIPUPDATECHECK);

		if (isOverwrite && isUpdate) {
			throw new IllegalArgumentException("Conflicting overwrite policies");
		}
		if (isDelete && !isOverwrite && !isUpdate) {
			throw new IllegalArgumentException(Options.DELETE.cmd
					+ " must be specified with " + Options.OVERWRITE + " or "
					+ Options.UPDATE + ".");
		}
		if (!isUpdate && skipCRC && skipTS) {
			throw new IllegalArgumentException(
					Options.SKIPCRC.cmd + " or " + 
					Options.SKIPTS + " is relevant only with the " +
					Options.UPDATE.cmd + " option");
		}
		return new Arguments(srcs, dst, log, flags, presevedAttributes,
				filelimit, sizelimit, mapredSslConf, extendedRegex, regexRoot, exportOnly,
				touchedDirs, fsCache);
	}

	/** {@inheritDoc} */
	public String toString() {
		return getClass().getName() + "{"
		+ "\n  srcs = " + srcs
		+ "\n  dst = " + dst
		+ "\n  log = " + log
		+ "\n  flags = " + flags
		+ "\n  preservedAttributes = " + preservedAttributes
		+ "\n  filelimit = " + filelimit
		+ "\n  sizelimit = " + sizelimit
		+ "\n  mapredSslConf = " + mapredSslConf
		+ "\n}";
	}
	
	private static int incrementAndCheckNotLastArg(int counter, String[] args, String except) 
			throws IllegalArgumentException {
		if (++counter ==  args.length) {
			throw new IllegalArgumentException(except);
		}
		return counter;
	}

	/**
	 * Get a list of file paths specified.
	 *
	 * @param fileSys
	 * @param dir
	 * @param filePattern
	 * @param suffix
	 * @return
	 * @throws IOException
	 */
	public static Collection<Path> getFilePaths(FileSystem dfs,
			final String dir, final String filePattern, final String prefix)
			throws IOException {
		Collection<Path> c = new HashSet<Path>();
		Path path = new Path(dir, filePattern);
		
		if (dfs.isDirectory(path)) {
			FileStatus stats[] = dfs.listStatus(path);
			if (stats != null && stats.length > 0) {
				for (FileStatus stat : stats) {
					if (stat.isDir()) {
						continue;
					}
					Path p = stat.getPath();
					if (p.getName().startsWith(prefix)) {
						c.add(p);
					}
				}
			}
		} else if (dfs.isFile(path)) {
			c.add(path);
		} else if (dfs.exists(path.getParent())) {

			final String pattern = path.getName();
			FileStatus stats[] = dfs.listStatus(path.getParent(),
					new PathFilter() {
						@Override
						public boolean accept(Path path) {
							return path.getName().matches(pattern);
						}
					});
			if (stats != null && stats.length > 0) {
				for (FileStatus stat : stats) {
					Path p = stat.getPath();
					c.add(p);
				}
			}
		}
		
		return c;
	}
	
	/**
	 * Given a file system, root dir, and a regex pattern, add all eligible paths to <code>paths</code>.
	 * 
	 * This can traverse the root dir to any depth and also handle regexes in directory names
	 * 
	 * @param root root dir
	 * @param pattern regex path with / as directory level separators
	 * @param fs file system
	 * @param paths found paths
	 * @param touchedDirs dirs that have been touched and may need preserving of attributes will be added to this set. 
	 * 		This can be null if not required.
	 * @param fsCache calling listStatus results in rpc calls and can lead to delays, especially when called on remote namenode.  This cache
	 * 		will be populated by the function to help later applications avoid extra rpc calls. This can be null if no cache is required.
	 * @throws IOException
	 */
	public static void findPaths(String root, String pattern, FileSystem fs, Collection<Path> paths, Set<FileStatus> touchedDirs,
			Map<String, FileStatus> fsCache) throws IOException {
		Set<Path> touchedDirsPathSet = new HashSet<Path>();

		FileStatus rootFile = null;
		try {
			rootFile = fs.getFileStatus(new Path(root));
		} catch (IOException e) {
			return;
		}
		
		
		String[] pathParts = pattern.split(REGEX_PATH_SPLITTER);
		// Precompile regexes
		Pattern[] regexes = new Pattern[pathParts.length];
		for (int x = 0; x < pathParts.length; x++) {
			regexes[x] = Pattern.compile(pathParts[x]);
		}
		
		// Create stack of files to process and depth into tree
		Stack<FileStatus> pathstack = new Stack<FileStatus>();
		Stack<Integer> depthStack = new Stack<Integer>();
		depthStack.push(0);
		pathstack.push(rootFile);
				
		while (!pathstack.empty()) {
			// Pop current directory and current depth
			FileStatus cur = pathstack.pop();
			int depth = depthStack.pop();
			
			// Iterate over children and check child against regex at current depth
			for (FileStatus child : fs.listStatus(cur.getPath())) {
				Matcher m = regexes[depth].matcher(child.getPath().getName());
				if (m.matches()) {
//					logger.info("Found path match between " + child.getPath() + " and " + pathParts[depth]);
					if (child.isDir()) {
						// If directory is at leaf of regex, add it as a source
						if (depth == pathParts.length - 1) {
							if (fsCache != null) fsCache.put(child.getPath().toString(), child);
							paths.add(child.getPath());
						// Otherwise push it onto stack and increment depth
						} else {
							if (touchedDirs != null) touchedDirs.add(child);
							pathstack.push(child);
							depthStack.push(depth + 1);
						}
					} else {
						
						//cache each leaf file's file status when regexPath option 
						// is present
						if (fsCache != null) fsCache.put(child.getPath().toString(), child);
						
						// Add every directory between this file and root to `touched' dirs
						// These dirs will have their attributes preserved after copying
						Path parentFolder = child.getPath().getParent();
						
						// We use a hashset of paths in addition to a hashset of Filestatuses
						// so we do not have to call fs.getFileStatus() as often
						
						while (parentFolder != null && 
								!parentFolder.toUri().getPath().equals(new Path(root).toUri().getPath())) {
							if (touchedDirs != null && !touchedDirsPathSet.contains(parentFolder)) {
//								logger.info("Adding " + parentFolder + " to touched paths");
								touchedDirs.add(fs.getFileStatus(parentFolder));
								touchedDirsPathSet.add(parentFolder);
							}
							parentFolder = parentFolder.getParent();
						}
						// Add file if it matches regex at current depth
						paths.add(child.getPath());
					}
				}
			}
		}
	}
	
}
