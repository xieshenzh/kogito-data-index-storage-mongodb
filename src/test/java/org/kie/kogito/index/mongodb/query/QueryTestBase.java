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

package org.kie.kogito.index.mongodb.query;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.kie.kogito.index.cache.Cache;
import org.kie.kogito.index.query.AttributeFilter;
import org.kie.kogito.index.query.AttributeSort;

import static org.assertj.core.api.Assertions.assertThat;

class QueryTestBase {

    static <K, V> void testQuery(Cache<K, V> cache, List<AttributeFilter<?>> filters, List<AttributeSort> sort, Integer offset, Integer limit, String... ids) {
        List<V> instances = cache.query().filter(filters).sort(sort).offset(offset).limit(limit).execute();
        if (sort != null) {
            assertThat(instances).hasSize(ids == null ? 0 : ids.length).extracting("id").containsExactly(ids);
        } else {
            assertThat(instances).hasSize(ids == null ? 0 : ids.length).extracting("id").containsExactlyInAnyOrder(ids);
        }
    }

    static void testSleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
            Assert.fail();
        }
    }
}
