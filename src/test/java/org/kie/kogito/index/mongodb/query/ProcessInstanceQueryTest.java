package org.kie.kogito.index.mongodb.query;

import java.time.Instant;
import java.util.UUID;

import javax.inject.Inject;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.kogito.index.cache.Cache;
import org.kie.kogito.index.cache.CacheService;
import org.kie.kogito.index.model.ProcessInstance;
import org.kie.kogito.index.mongodb.MongoDBServerTestResource;
import org.kie.kogito.index.mongodb.TestUtils;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.kie.kogito.index.model.ProcessInstanceState.ACTIVE;
import static org.kie.kogito.index.model.ProcessInstanceState.COMPLETED;
import static org.kie.kogito.index.mongodb.query.QueryTestBase.testQuery;
import static org.kie.kogito.index.query.QueryFilterFactory.and;
import static org.kie.kogito.index.query.QueryFilterFactory.between;
import static org.kie.kogito.index.query.QueryFilterFactory.contains;
import static org.kie.kogito.index.query.QueryFilterFactory.containsAll;
import static org.kie.kogito.index.query.QueryFilterFactory.containsAny;
import static org.kie.kogito.index.query.QueryFilterFactory.equalTo;
import static org.kie.kogito.index.query.QueryFilterFactory.greaterThan;
import static org.kie.kogito.index.query.QueryFilterFactory.greaterThanEqual;
import static org.kie.kogito.index.query.QueryFilterFactory.in;
import static org.kie.kogito.index.query.QueryFilterFactory.isNull;
import static org.kie.kogito.index.query.QueryFilterFactory.lessThan;
import static org.kie.kogito.index.query.QueryFilterFactory.lessThanEqual;
import static org.kie.kogito.index.query.QueryFilterFactory.like;
import static org.kie.kogito.index.query.QueryFilterFactory.notNull;
import static org.kie.kogito.index.query.QueryFilterFactory.or;

@QuarkusTest
@QuarkusTestResource(MongoDBServerTestResource.class)
public class ProcessInstanceQueryTest {

    @Inject
    CacheService cacheService;

    Cache<String, ProcessInstance> cache;

    @BeforeEach
    void setUp() {
        this.cache = cacheService.getProcessInstancesCache();
    }

    @AfterEach
    void tearDown() {
        cache.clear();
        Assert.assertTrue(cache.isEmpty());
    }

    @Test
    void test() {
        String processId = "travels";
        String processInstanceId = UUID.randomUUID().toString();
        String subProcessId = processId + "_sub";
        String subProcessInstanceId = UUID.randomUUID().toString();
        ProcessInstance processInstance = TestUtils.createProcessInstance(processInstanceId, processId, null, null, ACTIVE.ordinal());
        QueryTestBase.testSleep();
        ProcessInstance subProcessInstance = TestUtils.createProcessInstance(subProcessInstanceId, subProcessId, processInstanceId, processId, COMPLETED.ordinal());
        cache.put(processInstanceId, processInstance);
        cache.put(subProcessInstanceId, subProcessInstance);

        testQuery(cache, singletonList(in("state", asList(ACTIVE.ordinal(), COMPLETED.ordinal()))), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(equalTo("state", ACTIVE.ordinal())), null, null, null, processInstanceId);
        testQuery(cache, singletonList(greaterThan("state", ACTIVE.ordinal())), null, null, null, subProcessInstanceId);
        testQuery(cache, singletonList(greaterThanEqual("state", ACTIVE.ordinal())), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(lessThan("state", COMPLETED.ordinal())), null, null, null, processInstanceId);
        testQuery(cache, singletonList(lessThanEqual("state", COMPLETED.ordinal())), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(between("state", ACTIVE.ordinal(), COMPLETED.ordinal())), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(isNull("rootProcessInstanceId")), null, null, null, processInstanceId);
        testQuery(cache, singletonList(notNull("rootProcessInstanceId")), null, null, null, subProcessInstanceId);
        testQuery(cache, singletonList(contains("roles", "admin")), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(containsAny("roles", asList("admin", "kogito"))), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(containsAll("roles", asList("admin", "kogito"))), null, null, null);
        testQuery(cache, singletonList(like("processId", "*_sub")), null, null, null, subProcessInstanceId);
        testQuery(cache, singletonList(and(asList(lessThan("start", Instant.now().toEpochMilli()), lessThanEqual("start", Instant.now().toEpochMilli())))), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, singletonList(or(asList(equalTo("rootProcessInstanceId", processInstanceId), equalTo("start", processInstance.getStart().toInstant().toEpochMilli())))), null, null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, asList(isNull("roles"), isNull("end"), greaterThan("start", Instant.now().toEpochMilli()), greaterThanEqual("start", Instant.now().toEpochMilli())), null, null, null);

//        testQuery(cache, asList(in("id", asList(processInstanceId, subProcessInstanceId)), in("processId", asList(processId, subProcessId))), singletonList(orderBy("processId", SortDirection.ASC)), 1, 1, subProcessInstanceId);
//        testQuery(cache, null, singletonList(orderBy("processId", SortDirection.DESC)), null, null, processInstanceId, subProcessInstanceId);
        testQuery(cache, null, null, 1, 1, subProcessInstanceId);
//        testQuery(cache, null, singletonList(orderBy("processId", SortDirection.ASC)), 1, 1, subProcessInstanceId);
    }
}
