/*
 * Copyright 2020 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kie.kogito.index.mongodb.cache;

import org.junit.Assert;
import org.kie.kogito.index.cache.Cache;

public class CacheTestBase {

    static <K, V> void testCache(Cache<K, V> cache, K key, V value1, V value2) {
        Assert.assertNull(cache.get(key));

        cache.put(key, value1);
        Assert.assertEquals(value1, cache.get(key));

        cache.put(key, value2);
        Assert.assertEquals(value2, cache.get(key));

        cache.remove(key);
        Assert.assertNull(cache.get(key));
    }
}
