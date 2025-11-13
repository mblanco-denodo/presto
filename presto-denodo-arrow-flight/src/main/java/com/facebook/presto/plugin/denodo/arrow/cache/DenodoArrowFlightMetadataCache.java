/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.denodo.arrow.cache;

import com.facebook.presto.spi.ConnectorSession;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A generic cache that loads values on demand when {@link #get} is called.
 * Similar to LoadingCache from Google Guava but simplified.
 * Entries are automatically removed after the specified expiration time from their last access.
 *
 * @param <K> The type of the cache keys
 * @param <V> The type of the cached values
 */
public class DenodoArrowFlightMetadataCache<K, V>
{
    @FunctionalInterface
    public interface LoadingFunction<K, V>
    {
        V apply(K key, ConnectorSession session, Object... params) throws Exception;
    }

    private final Map<K, DenodoArrowFlightMetadataCacheEntry<V>> cache;
    private final LoadingFunction<K, V> loadingFunction;
    private final long expirationTimeMillis;

    /**
     * Creates a new cache with the specified loading function and default expiration of 5 seconds.
     */
    public DenodoArrowFlightMetadataCache(LoadingFunction<K, V> loadingFunction)
    {
        this(loadingFunction, 5);
    }

    /**
     * Creates a new cache with the specified loading function and expiration time.
     */
    public DenodoArrowFlightMetadataCache(LoadingFunction<K, V> loadingFunction, long expirationTimeSeconds)
    {
        this.cache = new ConcurrentHashMap<>();
        this.loadingFunction = loadingFunction;
        this.expirationTimeMillis = expirationTimeSeconds * 1000;

        // Set up a scheduled cleanup task
        ScheduledExecutorService cleanupExecutor = new ScheduledThreadPoolExecutor(1);
        cleanupExecutor.scheduleAtFixedRate(
                this::cleanupExpiredEntries,
                expirationTimeSeconds / 2,
                expirationTimeSeconds / 2,
                TimeUnit.SECONDS);
    }

    /**
     * Gets a value from the cache, loading it if necessary.
     * Updates the last access time for the entry.
     */
    public V get(K key, ConnectorSession session, Object... params) throws ExecutionException
    {
        DenodoArrowFlightMetadataCacheEntry<V> entry = cache.get(key);

        if (entry == null) {
            try {
                V value = loadingFunction.apply(key, session, params);
                if (value != null) {
                    entry = new DenodoArrowFlightMetadataCacheEntry<>(value);
                    cache.put(key, entry);
                    return value;
                }
            }
            catch (Exception e) {
                throw new ExecutionException("Failed to load value for key: " + key, e);
            }
            return null;
        }

        return entry.getValue();
    }

    /**
     * Returns the current size of the cache.
     *
     * @return The number of entries in the cache
     */
    public int size()
    {
        return cache.size();
    }

    /**
     * Removes entries that have expired (not accessed for the configured duration).
     */
    private void cleanupExpiredEntries()
    {
        long currentTime = System.currentTimeMillis();
        cache.entrySet().removeIf(entry ->
                (currentTime - entry.getValue().getLastAccessTime()) > expirationTimeMillis);
    }
}
