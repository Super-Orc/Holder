package com.loktar.loader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

public class DynamicClassLoader extends OrcoClassLoader {
	private static final Log LOG =
			LogFactory.getLog(DynamicClassLoader.class);

	// Dynamic jars are put under ${hbase.local.dir}/jars/
	private static final String DYNAMIC_JARS_DIR = File.separator
			+ "jars" + File.separator;

	//	private static final String DYNAMIC_JARS_DIR_KEY = "hdfs://rcspark2:9000/holder";
	private static final String DYNAMIC_JARS_DIR_KEY = "hdfs://localhost:9000/test";

	private File localDir;

	// FileSystem of the remote path, set only if remoteDir != null
	private FileSystem remoteDirFs;
	private Path remoteDir;

	// Last modified time of local jars ,also can as remote jar's last modified
	private HashMap<String, Long> jarModifiedTime;

	/**
	 * Creates a DynamicClassLoader that can load classes dynamically
	 * from jar files under a specific folder.
	 *
	 * @param conf   the configuration for the cluster.
	 * @param parent the parent ClassLoader to set.
	 */
	public DynamicClassLoader(final Configuration conf, final ClassLoader parent) {
		super(parent);
		initTempDir(conf);
	}

	public DynamicClassLoader(final ClassLoader parent) {
		super(parent);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		initTempDir(conf);
	}


	private synchronized void initTempDir(final Configuration conf) {
		jarModifiedTime = new HashMap<>();
		String localDirPath = DEFAULT_LOCAL_DIR + DYNAMIC_JARS_DIR;
		localDir = new File(localDirPath);
		if (!localDir.mkdirs() && !localDir.isDirectory()) {
			throw new RuntimeException("Failed to create local dir " + localDir.getPath()
					+ ", DynamicClassLoader failed to init");
		}

		String remotePath = DYNAMIC_JARS_DIR_KEY;
		if (remotePath.equals(localDirPath)) {
			remoteDir = null;  // ignore if it is the same as the local path
		} else {
			remoteDir = new Path(remotePath);
			try {
				remoteDirFs = remoteDir.getFileSystem(conf);
			} catch (IOException ioe) {
				LOG.warn("Failed to identify the fs of dir "
						+ remoteDir + ", ignored", ioe);
				remoteDir = null;
			}
		}
	}

	@Override
	public Class<?> loadClass(String name)
			throws ClassNotFoundException {
		try {
			return parent.loadClass(name);
		} catch (ClassNotFoundException e) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Class " + name + " not found - using dynamical class loader");
			}
			return tryRefreshClass(name);
		}
	}

	private Class<?> tryRefreshClass(String name)
			throws ClassNotFoundException {
		synchronized (getClassLoadingLock(name)) {
			// Check whether the class has already been loaded:
			Class<?> clasz = findLoadedClass(name);
			if (clasz != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Class " + name + " already loaded");
				}
			} else {
				try {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Finding class: " + name);
					}
					clasz = findClass(name);
				} catch (ClassNotFoundException cnfe) {
					// Load new jar files if any
					if (LOG.isDebugEnabled()) {
						LOG.debug("Loading new jar files, if any");
					}
					loadNewJars();

					if (LOG.isDebugEnabled()) {
						LOG.debug("Finding class again: " + name);
					}
					clasz = findClass(name);
				}
			}
			return clasz;
		}
	}

	private synchronized void loadNewJars() {
		// Refresh local jar file lists
		for (File file : localDir.listFiles()) {
			String fileName = file.getName();
			if (jarModifiedTime.containsKey(fileName)) {
				continue;
			}
			if (file.isFile() && fileName.endsWith(".jar")) {
				jarModifiedTime.put(fileName, file.lastModified());
				try {
					URL url = file.toURI().toURL();
					addURL(url);
				} catch (MalformedURLException mue) {
					// This should not happen, just log it
					LOG.warn("Failed to load new jar " + fileName, mue);
				}
			}
		}

		// Check remote files
		FileStatus[] statuses = null;
		if (remoteDir != null) {
			try {
				statuses = remoteDirFs.listStatus(remoteDir);
			} catch (IOException ioe) {
				LOG.warn("Failed to check remote dir status " + remoteDir, ioe);
			}
		}
		if (statuses == null || statuses.length == 0) {
			return; // no remote files at all
		}

		for (FileStatus status : statuses) {
			if (status.isDirectory()) continue; // No recursive lookup
			Path path = status.getPath();
			String fileName = path.getName();
			if (!fileName.endsWith(".jar")) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Ignored non-jar file " + fileName);
				}
				continue; // Ignore non-jar files
			}
			Long cachedLastModificationTime = jarModifiedTime.get(fileName);
			if (cachedLastModificationTime != null) {
				long lastModified = status.getModificationTime();
				if (lastModified < cachedLastModificationTime) {
					continue;
				}
			}
			try {
				// Copy it to local
				File dst = new File(localDir, fileName);
				remoteDirFs.copyToLocalFile(path, new Path(dst.getPath()));
				jarModifiedTime.put(fileName, dst.lastModified());
				URL url = dst.toURI().toURL();
				addURL(url);
			} catch (IOException ioe) {
				LOG.warn("Failed to load new jar " + fileName, ioe);
			}
		}
	}
}