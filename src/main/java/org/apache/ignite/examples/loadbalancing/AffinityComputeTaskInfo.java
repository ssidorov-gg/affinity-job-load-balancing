package org.apache.ignite.examples.loadbalancing;

public class AffinityComputeTaskInfo<T> {
    public static final String CACHE_NAME_SESS_ATTRIBUTE_KEY = "affinity-cache-name";

    private final String cacheName;

    private final T args;

    public AffinityComputeTaskInfo(final String cacheName, final T args) {
        this.cacheName = cacheName;
        this.args = args;
    }

    public String getCacheName() {
        return cacheName;
    }

    public T getArgs() {
        return args;
    }
}
