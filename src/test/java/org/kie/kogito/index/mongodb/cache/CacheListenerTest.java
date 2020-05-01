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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.kie.kogito.index.cache.Cache;
import org.kie.kogito.index.cache.CacheService;
import org.kie.kogito.index.model.ProcessInstance;
import org.kie.kogito.index.model.ProcessInstanceState;
import org.kie.kogito.index.mongodb.MongoDBServerTestResource;
import org.kie.kogito.index.mongodb.TestUtils;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(MongoDBServerTestResource.class)
public class CacheListenerTest {

    @Inject
    CacheService cacheService;

    @Test
    public static void testObjectCreatedListener(CacheService cacheService) throws Exception {
        String processId = "travels";
        String processInstanceId = UUID.randomUUID().toString();

        CompletableFuture<ProcessInstance> cf = new CompletableFuture<>();
        Cache<String, ProcessInstance> cache = cacheService.getProcessInstancesCache();
        cache.addObjectCreatedListener(pi -> cf.complete(pi));
        cache.put(processInstanceId, TestUtils.createProcessInstance(processInstanceId, processId, null, null, ProcessInstanceState.ACTIVE.ordinal()));

        ProcessInstance pi = cf.get(1, TimeUnit.MINUTES);
        assertThat(pi).hasFieldOrPropertyWithValue("id", processInstanceId).hasFieldOrPropertyWithValue("processId", processId);
    }

    @Test
    public static void testObjectUpdatedListener(CacheService cacheService) throws Exception {
        String processId = "travels";
        String processInstanceId = UUID.randomUUID().toString();

        CompletableFuture<ProcessInstance> cf = new CompletableFuture<>();
        Cache<String, ProcessInstance> cache = cacheService.getProcessInstancesCache();
        cache.addObjectUpdatedListener(pi -> cf.complete(pi));
        cache.put(processInstanceId, TestUtils.createProcessInstance(processInstanceId, processId, null, null, ProcessInstanceState.ACTIVE.ordinal()));
        cache.put(processInstanceId, TestUtils.createProcessInstance(processInstanceId, processId, null, null, ProcessInstanceState.COMPLETED.ordinal()));

        ProcessInstance pi = cf.get(1, TimeUnit.MINUTES);
        assertThat(pi).hasFieldOrPropertyWithValue("id", processInstanceId).hasFieldOrPropertyWithValue("state", ProcessInstanceState.COMPLETED.ordinal());
    }

    @Test
    public static void testObjectRemovedListener(CacheService cacheService) throws Exception {
        String processId = "travels";
        String processInstanceId = UUID.randomUUID().toString();

        CompletableFuture<String> cf = new CompletableFuture<>();
        Cache<String, ProcessInstance> cache = cacheService.getProcessInstancesCache();
        cache.addObjectRemovedListener(id -> cf.complete(id));
        cache.put(processInstanceId, TestUtils.createProcessInstance(processInstanceId, processId, null, null, ProcessInstanceState.ACTIVE.ordinal()));
        cache.remove(processInstanceId);

        String id = cf.get(1, TimeUnit.MINUTES);
        assertThat(id).isEqualTo(processInstanceId);
    }
}
