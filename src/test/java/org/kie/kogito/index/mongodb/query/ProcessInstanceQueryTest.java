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
import org.kie.kogito.index.query.SortDirection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.kie.kogito.index.model.ProcessInstanceState.ACTIVE;
import static org.kie.kogito.index.model.ProcessInstanceState.COMPLETED;
import static org.kie.kogito.index.mongodb.query.QueryTestBase.assertWithId;
import static org.kie.kogito.index.mongodb.query.QueryTestBase.assertWithIdInOrder;
import static org.kie.kogito.index.mongodb.query.QueryTestBase.queryAndAssert;
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
import static org.kie.kogito.index.query.QueryFilterFactory.orderBy;

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

        queryAndAssert(assertWithId(), cache, singletonList(in("state", asList(ACTIVE.ordinal(), COMPLETED.ordinal()))), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(equalTo("state", ACTIVE.ordinal())), null, null, null, processInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(greaterThan("state", ACTIVE.ordinal())), null, null, null, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(greaterThanEqual("state", ACTIVE.ordinal())), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(lessThan("state", COMPLETED.ordinal())), null, null, null, processInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(lessThanEqual("state", COMPLETED.ordinal())), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(between("state", ACTIVE.ordinal(), COMPLETED.ordinal())), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(isNull("rootProcessInstanceId")), null, null, null, processInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(notNull("rootProcessInstanceId")), null, null, null, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(contains("roles", "admin")), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(containsAny("roles", asList("admin", "kogito"))), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(containsAll("roles", asList("admin", "kogito"))), null, null, null);
        queryAndAssert(assertWithId(), cache, singletonList(like("processId", "*_sub")), null, null, null, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(and(asList(lessThan("start", Instant.now().toEpochMilli()), lessThanEqual("start", Instant.now().toEpochMilli())))), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, singletonList(or(asList(equalTo("rootProcessInstanceId", processInstanceId), equalTo("start", processInstance.getStart().toInstant().toEpochMilli())))), null, null, null, processInstanceId, subProcessInstanceId);
        queryAndAssert(assertWithId(), cache, asList(isNull("roles"), isNull("end"), greaterThan("start", Instant.now().toEpochMilli()), greaterThanEqual("start", Instant.now().toEpochMilli())), null, null, null);

        queryAndAssert(assertWithIdInOrder(), cache, asList(in("id", asList(processInstanceId, subProcessInstanceId)), in("processId", asList(processId, subProcessId))), singletonList(orderBy("processId", SortDirection.ASC)), 1, 1, subProcessInstanceId);
        queryAndAssert(assertWithIdInOrder(), cache, null, singletonList(orderBy("processId", SortDirection.DESC)), null, null, subProcessInstanceId, processInstanceId);
        queryAndAssert(assertWithIdInOrder(), cache, null, null, 1, 1, subProcessInstanceId);
        queryAndAssert(assertWithIdInOrder(), cache, null, asList(orderBy("processId", SortDirection.ASC), orderBy("state", SortDirection.ASC)), 1, 1, subProcessInstanceId);
    }
}
