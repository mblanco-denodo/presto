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

/**
 * A generic entry in a cache.
 * Similar to CacheEntry in guava but simplified.
 * @param <V> The type of the entry values
 */
public class DenodoArrowFlightMetadataCacheEntry<V>
{
    private final V value;
    private long lastAccessTime;

    DenodoArrowFlightMetadataCacheEntry(V value)
    {
        this.value = value;
        this.lastAccessTime = System.currentTimeMillis();
    }

    V getValue()
    {
        this.lastAccessTime = System.currentTimeMillis();
        return value;
    }

    long getLastAccessTime()
    {
        return lastAccessTime;
    }
}
