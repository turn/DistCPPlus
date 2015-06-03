/**
 * Copyright (C) 2013 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Special utility used for grabbing the FileStatuses of many input files
 * 
 * <p>
 * This utility reduces the number of RPC calls needed to grab many FileStatuses. This is
 * accomplished by grouping input paths by parent, and then using 1 RPC to get all the FileStatuses
 * of that parent. Because of this, the benefits are greatest when the input files share many common
 * parents. Similarly, there is no benefit to runtime if each input path has a different parent.
 * </p>
 * 
 * @author drubin
 */
public class FileStatusClusterOptimizer {
	private final Set<Path> rawInputSources;
	private Set<Path> inputParents;
	private Set<Path> inputRoots;
	private Map<Path, FileStatus> statusMapping;
	
	public FileStatusClusterOptimizer(List<Path> input) {
		if (input != null) {
			this.rawInputSources = new HashSet<Path>(input);
		} else {
			this.rawInputSources = new HashSet<Path>();
		}
	}
	
	/**
	 * Creates sets of parent and root paths based on the rawInputSources
	 */
	public void clusterSourcesIntoParents() {
		inputParents = new HashSet<Path>(rawInputSources.size());
		inputRoots = new HashSet<Path>();
		for (Path p : rawInputSources) {
			if (p != null) {
				// Get parent path
				Path parent = getSourceCluster(p);
				if (parent == null || parent.equals(p)) {
					// If parent is null, the path is already a filesystem root
					// However it seems that in our version of hadoop calling path.getParent()
					// on a root path just returns the path itself, not null, so we account for
					// that case as well
					inputRoots.add(p);
				} else {
					// Otherwise add its parent to the parent list
					inputParents.add(getSourceCluster(p));
				}
			}
		}
	}
	
	/**
	 * Generates a map of Path to FileStatus
	 * 
	 * <p>
	 * This will only work correctly if clusterSourcesIntoParents() has already been called
	 * </p>
	 * 
	 * @throws IOException
	 */
	public void generateFileStatuses() throws IOException {
		
		// Generate a throwaway Configuration that we will use to fetch FileSystems
		Configuration conf = new Configuration();
		
		// Construct our output map
		statusMapping = new HashMap<Path, FileStatus>();
		
		// Iterate over all clusters and process their children
		if (inputParents != null) {
			for (Path p : inputParents) {
				boolean parentHasHost = (p.toUri().getHost() != null);
				
				FileSystem fs = p.getFileSystem(conf);
				if (fs.exists(p)) {
					FileStatus[] statuses = fs.listStatus(p);
					
					// Iterate over children in each cluster and map child to input file if input file exists
					for (FileStatus file : statuses) {
						Path filePath = (parentHasHost ? file.getPath() : new Path(file.getPath().toUri().getPath()));
						if (rawInputSources.contains(filePath)) {
							statusMapping.put(filePath, file);
						}
					}
				}
			}
		}
		
		// Get filestatus of roots
		if (inputRoots != null) {
			for (Path p : inputRoots) {
				FileSystem fs = p.getFileSystem(conf);
				statusMapping.put(p, fs.getFileStatus(p));
			}
		}
	}
	
	/**
	 * Returns the parent of a path, the path itself (if its a root), or null if the input
	 * is null
	 * 
	 * @param p The path to get the parent of
	 * @return The parent of the path
	 */
	private Path getSourceCluster(Path p) {
		if (p == null || p.getParent() == null) {
			return p;
		} else {
			return p.getParent();
		}
	}
	
	public Map<Path, FileStatus> getFileStatusMappings() {
		return statusMapping;
	}
	
	public Set<Path> getInputFileSet() {
		return rawInputSources;
	}
	
	public Set<Path> getSourceParents() {
		return inputParents;
	}
	
	public Set<Path> getSourceRoots() {
		return inputRoots;
	}
}
