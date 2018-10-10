package com.loktar.loader;

import com.google.common.base.Preconditions;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentHashMap;

public class OrcoClassLoader extends URLClassLoader{

	// Maps class name to the corresponding lock object
	private final ConcurrentHashMap<String, Object> parallelLockMap
			= new ConcurrentHashMap<String, Object>();

//	protected static final String DEFAULT_LOCAL_DIR = "/opt/zf/holder/container-local-dir";
	protected static final String DEFAULT_LOCAL_DIR = "/tmp/container-local-dir";

	/**
	 * Parent class loader.
	 */
	protected final ClassLoader parent;

	/**
	 * Creates a DynamicClassLoader that can load classes dynamically
	 * from jar files under a specific folder.
	 *
	 * @param parent the parent ClassLoader to set.
	 */
	OrcoClassLoader(final ClassLoader parent) {
		super(new URL[]{}, parent);
		Preconditions.checkNotNull(parent, "No parent classloader!");
		this.parent = parent;
	}

	/**
	 * Returns the lock object for class loading operations.
	 */
	protected Object getClassLoadingLock(String className) {
		Object lock = parallelLockMap.get(className);
		if (lock != null) {
			return lock;
		}

		Object newLock = new Object();
		lock = parallelLockMap.putIfAbsent(className, newLock);
		if (lock == null) {
			lock = newLock;
		}
		return lock;
	}
}
