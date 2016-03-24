package com.turn.hadoop.distcp;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * A Map-reduce program to recursively copy directories between
 * different file-systems.
 *
 * NOTE: this file is mostly from org.apache.hadoop.tools.DistCp.
 */
public class DistCPPlus implements Tool {
    public static final Log LOG = LogFactory.getLog(DistCPPlus.class);

    static final String NAME = "DistCpPlus";

    private static final String usage = NAME
            + " [OPTIONS] <srcurl>* <desturl>" +
            "\n\nOPTIONS:" +
            "\n-p[rbugpt]             Preserve status" +
            "\n                       r: replication number" +
            "\n                       b: block size" +
            "\n                       u: user" +
            "\n                       g: group" +
            "\n                       p: permission" +
            "\n                       t: timestamp" +
            "\n                       -p alone is equivalent to -prbugp" +
            "\n-i                     Ignore failures" +
            "\n-log <logdir>          Write logs to <logdir>" +
            "\n-m <num_maps>          Maximum number of simultaneous copies" +
            "\n-overwrite             Overwrite destination" +
            "\n-update                Overwrite if src size different from dst size" +
            "\n-skipcrccheck          Do not use CRC check to determine if src is " +
            "\n-skiptscheck          Do not check last modified time stamp to determine if src is " +
            "\n                       different from dest. Relevant only if -update" +
            "\n                       is specified" +
            "\n-f <urilist_uri>       Use list at <urilist_uri> as src list" +
            "\n-rg <src/regex>        Use every file in src matching regex as a source" +
            "\n-regexPath <root> <rx> Takes root path and slash delimineted list of regexes." +
            "\n                       Recursively look at children of root and matches files with" +
            "                         their corresponding depth into the given regex" +
            "\n-filelimit <n>         Limit the total number of files to be <= n" +
            "\n-sizelimit <n>         Limit the total size to be <= n bytes" +
            "\n-delete                Delete the files existing in the dst but not in src" +
            "\n-mapredSslConf <f>     Filename of SSL configuration for mapper task" +
            "\n-mapper <f>     		  The full class name of the mapper used for filtering purpose" +
            "\n-market <f>     		  An integer as market ID used for market specific tasks" +

            "\n\nNOTE 1: if -overwrite or -update are set, each source URI is " +
            "\n      interpreted as an isomorphic update to an existing directory." +
            "\nFor example:" +
            "\nhadoop " + NAME + " -p -update \"hdfs://A:8020/user/foo/bar\" " +
            "\"hdfs://B:8020/user/foo/baz\"\n" +
            "\n     would update all descendants of 'baz' also in 'bar'; it would " +
            "\n     *not* update /user/foo/baz/bar" +

            "\n\nNOTE 2: The parameter <n> in -filelimit and -sizelimit can be " +
            "\n     specified with symbolic representation.  For examples," +
            "\n       1230k = 1230 * 1024 = 1259520" +
            "\n       891g = 891 * 1024^3 = 956703965184" +

            "\n\nNOTE 3: When using -regexPath the root given is assumed to be the " +
            "\n     root of all source files. So do not use it with other source" +
            "\n     calculating options or with other -regexPath options unless you" +
            "\n     are using the same root" +

            "\n";

    private static final long BYTES_PER_MAP =  256 * 1024 * 1024;
    private static final int MAX_MAPS_PER_NODE = 20;
    private static final int SYNC_FILE_MAX = 10;
    private static final int MAX_SOURCE_SIZE_FOR_PRINTING = 30;

    private static JobConf lastProcessedJob;

    public static enum Counter { COPY, SKIP, FAIL, BYTESCOPIED, BYTESEXPECTED, RECORDSKIPPED }
    
    public static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
    public static final String DST_DIR_LABEL = NAME + ".dest.path";
    public static final String JOB_DIR_LABEL = NAME + ".job.dir";
    public static final String MAX_MAPS_LABEL = NAME + ".max.map.tasks";
    public static final String SRC_LIST_LABEL = NAME + ".src.list";
    public static final String SRC_COUNT_LABEL = NAME + ".src.count";
    public static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
    public static final String DST_DIR_LIST_LABEL = NAME + ".dst.dir.list";
    public static final String BYTES_PER_MAP_LABEL = NAME + ".bytes.per.map";
    public static final String PRESERVE_STATUS_LABEL
            = Options.PRESERVE_STATUS.propertyname + ".value";
    public static final String COPY_MAPPER_CLASS_NAME_LABLE = NAME + ".copy.mapper.class.name";
    public static final String MARKET_ID_LABEL = NAME + ".market.id";

    public static final String SRC_LISTFILE_NAME = "_distcp_src_files";
    public static final String DST_LISTFILE_NAME = "_distcp_dst_files";
    public static final String DST_DIR_LISTFILE_NAME = "_distcp_dst_dirs";
    // Metric keys for accessing metrics stored in the JobConf
    public static final String SETUP_TIME = NAME + ".time.setup";
    public static final String RUN_TIME = NAME + ".time.run";
    public static final String CLEANUP_TIME = NAME + ".time.cleanup";
    public static final String RUN_SUCCESS = NAME + "isSuccessful";

    private JobConf conf;
    private Map<FilePair, Long> file_pair_type_index = new HashMap<FilePair, Long>();
    private boolean is_real = true;

    @Override
    public void setConf(Configuration conf) {
        if (conf instanceof JobConf) {
            this.conf = (JobConf) conf;
        } else {
            this.conf = new JobConf(conf);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public DistCPPlus(Configuration conf, boolean isReal) {
        setConf(conf);
        is_real = isReal;
    }

    /**
     * Driver to copy srcPath to destPath depending on required protocol.
     * @param args arguments
     */
    static void copy(final Configuration conf, final Arguments args,
                     Map<FilePair, Long> filePairTypeIndex, boolean isReal) throws IOException {
        // Printing the entire source input array with too many inputs can deplete heap space
        if (args.srcs.size() < MAX_SOURCE_SIZE_FOR_PRINTING) {
            LOG.info("srcPaths=" + args.srcs);
        } else {
            LOG.info("srcPaths size: " + args.srcs.size());
        }
        LOG.info("destPath=" + args.dst);

        //A default copier is assigned for simplification
        JobConf job = createJobConf(conf);
        job.setJobName(NAME + " " + args.dst.toUri().toString());
        lastProcessedJob = job;

		
		/* when -regexPath is used, do not check existence of file
		 * we will check them in setup()
		 * if we call checkSrcPath(), it will call fs.exists(path)
		 * for each file, which is too expensive 
		 */
        boolean extendedRegex = args.extendedRegex;
        if(!extendedRegex) {
            DistCpUtils.checkSrcPath(job, args.srcs);
        }

        if (args.preservedAttributes != null) {
            job.set(PRESERVE_STATUS_LABEL, args.preservedAttributes);
        }
        if (args.mapredSslConf != null) {
            job.set("dfs.https.client.keystore.resource", args.mapredSslConf);
        }


        long cleanupStartTime = -1;

        // Initialize the mapper
        try {
            // Time setup() function and store time taken in JobConf
            long setupStartTime = System.currentTimeMillis();
            boolean setupResult = setup(conf, job, args, filePairTypeIndex);
            conf.set(SETUP_TIME, String.valueOf(System.currentTimeMillis() - setupStartTime));

            if (setupResult) {
                if (!args.exportJob) {
                    if (isReal) {
                        // Time and run job, store jobs time taken in JobConf
                        long runStartTime = System.currentTimeMillis();
                        try {
                            JobClient.runJob(job);
                            conf.setBoolean(RUN_SUCCESS, true);
                        } catch (IOException e) {
                            conf.setBoolean(RUN_SUCCESS, false);
                            LOG.error("Exception while copying file " + args.srcs, e);
                        }
                        conf.set(RUN_TIME, String.valueOf(System.currentTimeMillis() - runStartTime));
                    }
                    // Time cleanup starting from finalize()
                    cleanupStartTime = System.currentTimeMillis();
                    finalize(conf, job, args.dst, args.preservedAttributes);
                }
            }
        } finally {
            if (!args.exportJob) {
                cleanupJob(job);
                conf.set(CLEANUP_TIME, String.valueOf(System.currentTimeMillis() - cleanupStartTime));
            }
        }
    }

    public static void updatePermissions(FileStatus src, FileStatus dst,
                                         EnumSet<FileAttribute> preseved, FileSystem destFileSys
    ) throws IOException {
        String owner = null;
        String group = null;
        if (preseved.contains(FileAttribute.USER)
                && !src.getOwner().equals(dst.getOwner())) {
            owner = src.getOwner();
        }
        if (preseved.contains(FileAttribute.GROUP)
                && !src.getGroup().equals(dst.getGroup())) {
            group = src.getGroup();
        }
        if (owner != null || group != null) {
            destFileSys.setOwner(dst.getPath(), owner, group);
        }
        if (preseved.contains(FileAttribute.PERMISSION)
                && !src.getPermission().equals(dst.getPermission())) {
            destFileSys.setPermission(dst.getPath(), src.getPermission());
        }
        if (preseved.contains(FileAttribute.TIMESTAMP) && !dst.isDir())
        {
            //FIXME(drubin) https://issues.apache.org/jira/browse/HDFS-2436
            // setTimes() does not work with directories. This may be fixed in newer hadoop but
            // for now it thinks the directory does not exist
            destFileSys.setTimes(dst.getPath(),
                    src.getModificationTime(), src.getAccessTime());
        }
    }

    public static void finalize(Configuration conf, JobConf jobconf,
                                final Path destPath, String presevedAttributes) throws IOException {
        if (presevedAttributes == null) {
            return;
        }
        EnumSet<FileAttribute> preseved = FileAttribute.parse(presevedAttributes);
        if (!preseved.contains(FileAttribute.USER)
                && !preseved.contains(FileAttribute.GROUP)
                && !preseved.contains(FileAttribute.PERMISSION)) {
            return;
        }
        LOG.info("Preserved attributes: " + preseved.toString());

        FileSystem dstfs = destPath.getFileSystem(conf);
        Path dstdirlist = new Path(jobconf.get(DST_DIR_LIST_LABEL));
        SequenceFile.Reader in = null;
        try {
            in = new SequenceFile.Reader(dstdirlist.getFileSystem(jobconf),
                    dstdirlist, jobconf);
            Text dsttext = new Text();
            FilePair pair = new FilePair();
            for(; in.next(dsttext, pair); ) {
                Path absdst = new Path(destPath, pair.output);
                LOG.info("Preserving attributes for file pair: " + pair.input.getPath() + ", " + absdst);
                updatePermissions(pair.input, dstfs.getFileStatus(absdst),
                        preseved, dstfs);
            }
        } catch (Exception e) {
            LOG.error("Exception while preserving attributes for " + destPath, e);
        } finally {

            DistCpUtils.checkAndClose(in);
        }
    }

    /**
     * This is the main driver for recursively copying directories
     * across file systems. It takes at least two cmdline parameters. A source
     * URL and a destination URL. It then essentially does an "ls -lR" on the
     * source URL, and writes the output in a round-robin manner to all the map
     * input files. The mapper actually copies the files allotted to it. The
     * reduce is empty.
     */
    @Override
    public int run(String[] args) {
        try {
            copy(conf, Arguments.valueOf(args, conf), file_pair_type_index, is_real);
            return 0;
        } catch (IllegalArgumentException e) {
            System.err.println(StringUtils.stringifyException(e) + "\n" + usage);
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        } catch (DuplicationException e) {
            System.err.println(StringUtils.stringifyException(e));
            return DuplicationException.ERROR_CODE;
        } catch (RemoteException e) {
            final IOException unwrapped = e.unwrapRemoteException(
                    FileNotFoundException.class,
                    AccessControlException.class,
                    QuotaExceededException.class);
            System.err.println(StringUtils.stringifyException(unwrapped));
            return -3;
        } catch (Exception e) {
            System.err.println("With failures, global counters are inaccurate; " +
                    "consider running with -i");
            System.err.println("Copy failed: " + StringUtils.stringifyException(e));
            LOG.error(e);
            return -999;
        }
    }

    public boolean hasFileCopied()
    {
        return !file_pair_type_index.isEmpty();
    }

    public List<FileStatus> getSourceFiles4Transfer()
    {
        List<FileStatus> ret = new ArrayList<FileStatus>();

        for(FilePair cur_file_pair : file_pair_type_index.keySet())
        {
            if(file_pair_type_index.get(cur_file_pair) > 0) // is a file
            {
                ret.add(cur_file_pair.input);
            }
        }

        return ret;
    }

    /**
     * @return The JobConf of the last ran distcp++ job
     */
    public static JobConf getLastRanJob() {
        return lastProcessedJob;
    }

    public static void main(String[] args) throws Exception {
        JobConf job = new JobConf(DistCPPlus.class);
        DistCPPlus distcp = new DistCPPlus(job, true);
        int res = ToolRunner.run(distcp, args);
        System.exit(res);
    }

    /**
     * Runs distcp++, but only generates a JobConf and does not actually submit the job.
     * @param args Command line arguments to setup the job
     * @return A jobconf representing the job
     */
    public static JobConf generateConf(String[] args) throws Exception {
        String[] newArgs = (String[]) ArrayUtils.addAll(new String[]{"-exportOnly"}, args);
        JobConf job = new JobConf(DistCPPlus.class);
        DistCPPlus distcp = new DistCPPlus(job, true);
        int res = ToolRunner.run(distcp, newArgs);
        if (res != 0) {
            throw new RuntimeException("Got error code " + String.valueOf(res));
        }
        return lastProcessedJob;
    }

    /**
     * Cleans up temporary files left by a job
     * @param job The JobConf representing the distcp++ map/reduce job to cleanup
     */
    public static void cleanupJob(JobConf job) throws IOException {
        cleanupJob(job, job);
    }

    /**
     * Cleans up temporary files left by a distcp++ job using a given config's FileSystem
     * @param job The JobConf representing the distcp++ map/reduce job to cleanup
     * @param config Configuration whose FileSystem will be used to do the cleaning
     */
    public static void cleanupJob(JobConf job, Configuration config) throws IOException {
        //delete tmp
        DistCpUtils.fullyDelete(job.get(TMP_DIR_LABEL), config);
        //delete jobDirectory
        DistCpUtils.fullyDelete(job.get(JOB_DIR_LABEL), config);
    }

    /**
     * Make a path relative with respect to a root path.
     * absPath is always assumed to descend from root.
     * Otherwise returned path is null.
     */
    static String makeRelative(Path root, Path absPath) {
        if (!absPath.isAbsolute()) {
            throw new IllegalArgumentException("!absPath.isAbsolute(), absPath="
                    + absPath);
        }
        String p = absPath.toUri().getPath();

        StringTokenizer pathTokens = new StringTokenizer(p, "/");
        for(StringTokenizer rootTokens = new StringTokenizer(
                root.toUri().getPath(), "/"); rootTokens.hasMoreTokens(); ) {
            if (!rootTokens.nextToken().equals(pathTokens.nextToken())) {
                return null;
            }
        }
        StringBuilder sb = new StringBuilder();
        for(; pathTokens.hasMoreTokens(); ) {
            sb.append(pathTokens.nextToken());
            if (pathTokens.hasMoreTokens()) { sb.append(Path.SEPARATOR); }
        }
        return sb.length() == 0? ".": sb.toString();
    }

    /**
     * Calculate how many maps to run.
     * Number of maps is bounded by a minimum of the cumulative size of the
     * copy / (distcp.bytes.per.map, default BYTES_PER_MAP or -m on the
     * command line) and at most (distcp.max.map.tasks, default
     * MAX_MAPS_PER_NODE * nodes in the cluster).
     * @param totalBytes Count of total bytes for job
     * @param job The job to configure
     * @return Count of maps to run.
     */
    private static void setMapCount(long totalBytes, JobConf job)
            throws IOException {

        int numMaps =
                (int)(totalBytes / job.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP));
        numMaps = Math.min(numMaps,
                job.getInt(MAX_MAPS_LABEL, MAX_MAPS_PER_NODE *
                        new JobClient(job).getClusterStatus().getTaskTrackers()));
        job.setNumMapTasks(Math.max(numMaps, 1));
    }

    //Job configuration
    private static JobConf createJobConf(Configuration conf)
    {
        JobConf jobconf = new JobConf(conf, DistCPPlus.class);
        jobconf.setJobName(NAME);

        // turn off speculative execution, because DFS doesn't handle
        // multiple writers to the same file.
        jobconf.setMapSpeculativeExecution(false);

        jobconf.setInputFormat(CopyInputFormat.class);
        jobconf.setOutputKeyClass(Text.class);
        jobconf.setOutputValueClass(Text.class);

        Class mapperClass = DefaultCopyFilesMapper.class; // a default class is given
        String mapper_class_name = conf.get(COPY_MAPPER_CLASS_NAME_LABLE);
        if(mapper_class_name != null)
        {
            try
            {
                mapperClass = Class.forName(mapper_class_name);
            } catch (ClassNotFoundException e)
            {
                throw new IllegalArgumentException("The mapper class "+
                        mapper_class_name+" doesn't exist!");
            }
        }
        jobconf.setMapperClass(mapperClass);
        jobconf.setNumReduceTasks(0);
        return jobconf;
    }

    private static final Random RANDOM = new Random();
    public static String getRandomId() {
        return Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE), 36);
    }

    /**
     * Initialize DFSCopyFileMapper specific job-configuration.
     * @param conf : The dfs/mapred configuration.
     * @param jobConf : The handle to the jobConf object to be initialized.
     * @param args Arguments
     * @return true if it is necessary to launch a job.
     */
    private static boolean setup(Configuration conf, JobConf jobConf,
                                 final Arguments args, Map<FilePair,Long> filePairTypeMap)
            throws IOException {
        jobConf.set(DST_DIR_LABEL, args.dst.toUri().toString());

        //set boolean values
        final boolean update = args.flags.contains(Options.UPDATE);
        final boolean skipCRCCheck = args.flags.contains(Options.SKIPCRC);
        final boolean skipTSCheck = args.flags.contains(Options.SKIPTS);
        final boolean skipUpdateCheck = args.flags.contains(Options.SKIPUPDATECHECK);
        final boolean overwrite = !update && args.flags.contains(Options.OVERWRITE);
        final boolean extendedRegex = args.extendedRegex;
        final Path regexRoot = args.regexRoot;
        jobConf.setBoolean(Options.UPDATE.propertyname, update);
        jobConf.setBoolean(Options.SKIPCRC.propertyname, skipCRCCheck);
        jobConf.setBoolean(Options.SKIPTS.propertyname, skipTSCheck);
        jobConf.setBoolean(Options.SKIPUPDATECHECK.propertyname, skipUpdateCheck);
        jobConf.setBoolean(Options.OVERWRITE.propertyname, overwrite);
        jobConf.setBoolean(Options.IGNORE_READ_FAILURES.propertyname,
                args.flags.contains(Options.IGNORE_READ_FAILURES));
        jobConf.setBoolean(Options.PRESERVE_STATUS.propertyname,
                args.flags.contains(Options.PRESERVE_STATUS));

        final String randomId = getRandomId();
        JobClient jClient = new JobClient(jobConf);
//        Path stagingArea;
//        try {
//            stagingArea = JobSubmissionFiles.getStagingDir(jClient, conf);
//        } catch (InterruptedException e) {
//            throw new IOException(e);
//        }
        String stagingArea = "/tmp/distcp/";
        Path jobDirectory = new Path(stagingArea + NAME + "_" + randomId);
        FsPermission mapredSysPerms =
                new FsPermission(JobSubmissionFiles.JOB_DIR_PERMISSION);
        FileSystem.mkdirs(jClient.getFs(), jobDirectory, mapredSysPerms);
        jobConf.set(JOB_DIR_LABEL, jobDirectory.toString());

        long maxBytesPerMap = conf.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP);

        FileSystem dstfs = args.dst.getFileSystem(conf);

        // get tokens for all the required FileSystems..
        TokenCache.obtainTokensForNamenodes(jobConf.getCredentials(),
                new Path[] {args.dst}, conf);

//		System.out.println("TS1 = "+(System.currentTimeMillis()-ts));
//		ts = System.currentTimeMillis();

        boolean dstExists = dstfs.exists(args.dst);
        boolean dstIsDir = false;
        if (dstExists) {
            dstIsDir = dstfs.getFileStatus(args.dst).isDir();
        }

//		System.out.println("TS2 = "+(System.currentTimeMillis()-ts));
//		ts = System.currentTimeMillis();

        // default logPath
        Path logPath = args.log;
        if (logPath == null) {
            String filename = "_distcp_logs_" + randomId;
            if (!dstExists || !dstIsDir) {
                Path parent = args.dst.getParent();
                if (null == parent) {
                    // If dst is '/' on S3, it might not exist yet, but dst.getParent()
                    // will return null. In this case, use '/' as its own parent to prevent
                    // NPE errors below.
                    parent = args.dst;
                }
                if (!dstfs.exists(parent)) {
                    dstfs.mkdirs(parent);
                }
                logPath = new Path(parent, filename);
            } else {
                logPath = new Path(args.dst, filename);
            }
        }
        FileOutputFormat.setOutputPath(jobConf, logPath);

        // create src list, dst list
        FileSystem jobfs = jobDirectory.getFileSystem(jobConf);

        Path srcfilelist = new Path(jobDirectory, SRC_LISTFILE_NAME);
        jobConf.set(SRC_LIST_LABEL, srcfilelist.toString());
        SequenceFile.Writer src_writer = SequenceFile.createWriter(jobfs, jobConf,
                srcfilelist, LongWritable.class, FilePair.class,
                SequenceFile.CompressionType.NONE);

        Path dstfilelist = new Path(jobDirectory, DST_LISTFILE_NAME);
        SequenceFile.Writer dst_writer = SequenceFile.createWriter(jobfs, jobConf,
                dstfilelist, Text.class, Text.class,
                SequenceFile.CompressionType.NONE);

        Path dstdirlist = new Path(jobDirectory, DST_DIR_LISTFILE_NAME);
        jobConf.set(DST_DIR_LIST_LABEL, dstdirlist.toString());
        SequenceFile.Writer dir_writer = SequenceFile.createWriter(jobfs, jobConf,
                dstdirlist, Text.class, FilePair.class,
                SequenceFile.CompressionType.NONE);

//		System.out.println("TS3 = "+(System.currentTimeMillis()-ts));
//		ts = System.currentTimeMillis();

        // handle the case where the destination directory doesn't exist
        // and we've only a single src directory OR we're updating/overwriting
        // the contents of the destination directory.
        final boolean special =
                (args.srcs.size() == 1 && !dstExists) || update || overwrite;
        int srcCount = 0, cnsyncf = 0, dirsyn = 0;
        long fileCount = 0L, byteCount = 0L, cbsyncs = 0L;

        Map<String, FileStatus>  fsCache = args.getFileStatusCache();

        try {
            for(Iterator<Path> srcItr = args.srcs.iterator(); srcItr.hasNext(); ) {
                // Get source file, its filesystem, and its filestatus
                final Path src = srcItr.next();
                FileSystem srcfs = null;
                FileStatus srcfilestat = null;

                //when regex option is used, lookup cache for
                //file status object
                if (extendedRegex && fsCache != null) {
                    srcfilestat = fsCache.get(src.toString());
                }

                srcfs = src.getFileSystem(conf);
                //in case does not hit the cache
                if (srcfilestat == null) {
                    srcfilestat = srcfs.getFileStatus(src);
                }



                Path root;
                if (extendedRegex) {
                    root = regexRoot;
                } else {
                    root = special && srcfilestat.isDir() ? src : src.getParent();
                }
                if (srcfilestat.isDir()) {
                    ++srcCount;
                }

//				System.out.println("TS4.2 = "+(System.currentTimeMillis()-ts));
//				ts = System.currentTimeMillis();

                Stack<FileStatus> pathstack = new Stack<FileStatus>();
                for(pathstack.push(srcfilestat); !pathstack.empty(); ) {

                    FileStatus cur = pathstack.pop();
                    FileStatus[] children = null;

                    if (extendedRegex && fsCache != null) {

                        FileStatus tmpFs  = fsCache.get(cur.getPath().toString());
                        if (tmpFs != null && !tmpFs.isDir()){
                            children = new FileStatus[1];
                            children[0]= tmpFs;
                        }
                    }

                    if (children == null) {
                        children = srcfs.listStatus(cur.getPath());
                    }

                    for(FileStatus child : children) {
                        boolean skipfile = false;
                        final String dst = makeRelative(root, child.getPath());
                        ++srcCount;

//						System.out.println("TS4.3.2 = "+(System.currentTimeMillis()-ts));
//						ts = System.currentTimeMillis();

                        if (child.isDir()) {
                            pathstack.push(child);
                        }
                        else {

                            //skip file if it exceed file limit or size limit
                            skipfile = fileCount == args.filelimit
                                    || byteCount + child.getLen() > args.sizelimit;

                            //skip file if the src and the dst files are the same.
                            if(update)
                            {
                                FileStatus dstfilestatus = null;
                                boolean isSame = false;
                                Path dst_path = new Path(args.dst, dst);
                                if (dstfs.exists(dst_path)) {
                                    try {
                                        dstfilestatus = dstfs.getFileStatus(dst_path);
                                    } catch(FileNotFoundException fnfe) {
                                        // do nothing
                                        dstfilestatus = null;
                                    }
                                    //
                                    isSame = dstfilestatus == null ? false :
                                            DistCpUtils.sameFile(srcfs, child, dstfs,
                                                    dstfilestatus, skipCRCCheck, skipTSCheck);
                                }

                                skipfile = isSame;
                            }


                            if (!skipfile) {
                                ++fileCount;
                                byteCount += child.getLen();

                                if (LOG.isTraceEnabled())
                                {
                                    LOG.trace("adding file " + child.getPath());
                                }

//								System.out.println("adding file " + child.getPath());

                                ++cnsyncf;
                                cbsyncs += child.getLen();
                                if (cnsyncf > SYNC_FILE_MAX || cbsyncs > maxBytesPerMap) {
                                    src_writer.sync();
                                    dst_writer.sync();
                                    cnsyncf = 0;
                                    cbsyncs = 0L;
                                }
                            }
                        }

                        if (!skipfile) {
                            long file_type = child.isDir()? 0: child.getLen();
                            FilePair file_pair = new FilePair(child, dst);
                            src_writer.append(new LongWritable(file_type), file_pair);
                            filePairTypeMap.put(file_pair, file_type);
                        }

                        dst_writer.append(new Text(dst),
                                new Text(child.getPath().toString()));

//						System.out.println("TS4.3.4 = "+(System.currentTimeMillis()-ts));
//						ts = System.currentTimeMillis();
                    }

                    // If src is a directory, add it to list of dirs to preserve attributes
                    if (cur.isDir()) {
                        String dst = makeRelative(root, cur.getPath());
                        dir_writer.append(new Text(dst), new FilePair(cur, dst));
                        LOG.info("Added " + dst + " to paths to preserve status");
                        if (++dirsyn > SYNC_FILE_MAX) {
                            dirsyn = 0;
                            dir_writer.sync();
                        }
                    }
                }
            }

            // If using -regexPath option, add all directories touched by regex to list of
            // directories to preserve their attributes
            if (extendedRegex) {
                if (args.regexTouchedDirectories != null) {
                    for (FileStatus touchedDir : args.regexTouchedDirectories) {
                        String dst = makeRelative(regexRoot, touchedDir.getPath());
                        dir_writer.append(new Text(dst), new FilePair(touchedDir, dst));
                        LOG.info("Added " + dst + " to paths to preserve status");
                        if (++dirsyn > SYNC_FILE_MAX) {
                            dirsyn = 0;
                            dir_writer.sync();
                        }
                    }
                }

//				System.out.println("TS4.3 = "+(System.currentTimeMillis()-ts));
//				ts = System.currentTimeMillis();
            }

            //release cache to release some memory.
            args.clearFileStatusCache();

        } finally {
            DistCpUtils.checkAndClose(src_writer);
            DistCpUtils.checkAndClose(dst_writer);
            DistCpUtils.checkAndClose(dir_writer);
        }

//		System.out.println("TS4.4 = "+(System.currentTimeMillis()-ts));
//		ts = System.currentTimeMillis();

        FileStatus dststatus = null;
        try {
            dststatus = dstfs.getFileStatus(args.dst);
        } catch(FileNotFoundException fnfe) {
            LOG.info(args.dst + " does not exist.");
        }

        // create dest path dir if copying > 1 file
        if (dststatus == null) {
            if (srcCount > 1 && !dstfs.mkdirs(args.dst)) {
                throw new IOException("Failed to create" + args.dst);
            }
        }

        final Path sorted = new Path(jobDirectory, "_distcp_sorted");
        DistCpUtils.checkDuplication(jobfs, dstfilelist, sorted, conf);

        if (dststatus != null && args.flags.contains(Options.DELETE)) {
            DistCpUtils.deleteNonexisting(dstfs, dststatus, sorted,
                    jobfs, jobDirectory, jobConf, conf);
        }

        Path tmpDir = new Path(
                (dstExists && !dstIsDir) || (!dstExists && srcCount == 1)?
                        args.dst.getParent(): args.dst, "_distcp_tmp_" + randomId);

        System.out.println("TMP DIR: "+tmpDir);
        jobConf.set(TMP_DIR_LABEL, tmpDir.toUri().toString());

        // Explicitly create the tmpDir to ensure that it can be cleaned
        // up by fullyDelete() later.
        tmpDir.getFileSystem(conf).mkdirs(tmpDir);

        LOG.info("sourcePathsCount=" + srcCount);
        LOG.info("filesToCopyCount=" + fileCount);
        LOG.info("bytesToCopyCount=" + StringUtils.humanReadableInt(byteCount));
        jobConf.setInt(SRC_COUNT_LABEL, srcCount);
        jobConf.setLong(TOTAL_SIZE_LABEL, byteCount);
        setMapCount(byteCount, jobConf);

//		System.out.println("TS4.5 = "+(System.currentTimeMillis()-ts));
//		ts = System.currentTimeMillis();

        return fileCount > 0;
    }

}
