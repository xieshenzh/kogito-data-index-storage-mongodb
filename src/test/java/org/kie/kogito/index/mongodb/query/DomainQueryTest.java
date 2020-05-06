package org.kie.kogito.index.mongodb.query;

import java.util.UUID;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.kogito.index.cache.Cache;
import org.kie.kogito.index.cache.CacheService;
import org.kie.kogito.index.mongodb.MongoDBServerTestResource;
import org.kie.kogito.index.mongodb.TestUtils;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.kie.kogito.index.mongodb.query.QueryTestBase.assertWithObjectNode;
import static org.kie.kogito.index.mongodb.query.QueryTestBase.assertWithObjectNodeInOrder;
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

@QuarkusTest
@QuarkusTestResource(MongoDBServerTestResource.class)
public class DomainQueryTest {

    @Inject
    CacheService cacheService;

    Cache<String, ObjectNode> cache;

    @BeforeEach
    void setUp() {
        this.cache = cacheService.getDomainModelCache("travels");
    }

    @AfterEach
    void tearDown() {
        cache.clear();
        Assert.assertTrue(cache.isEmpty());
    }

    @Test
    void test() {
        String processInstanceId1 = UUID.randomUUID().toString() + "_process1";
        String processInstanceId2 = UUID.randomUUID().toString();

        ObjectNode node1 = TestUtils.createDomainData(processInstanceId1, "John", "Doe");
        QueryTestBase.testSleep();
        ObjectNode node2 = TestUtils.createDomainData(processInstanceId2, "Jane", "Toe");
        cache.put(processInstanceId1, node1);
        cache.put(processInstanceId2, node2);

        queryAndAssert(assertWithObjectNode(), cache, singletonList(in("traveller.firstName", asList("John", "Jane"))), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(equalTo("traveller.firstName", "John")), null, null, null, processInstanceId1);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(greaterThan("traveller.age", 27)), null, null, null);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(greaterThanEqual("traveller.age", 27)), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(lessThan("traveller.age", 27)), null, null, null);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(lessThanEqual("traveller.age", 27)), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(between("traveller.age", 27, 28)), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(isNull("traveller.age")), null, null, null);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(notNull("traveller.age")), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(contains("_id", processInstanceId2)), null, null, null, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(containsAny("_id", asList(processInstanceId1, processInstanceId2))), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(containsAll("_id", asList(processInstanceId1, processInstanceId2))), null, null, null);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(like("traveller.firstName", "*hn")), null, null, null, processInstanceId1);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(and(asList(equalTo("traveller.firstName", "John"), equalTo("traveller.lastName", "Doe")))), null, null, null, processInstanceId1);
        queryAndAssert(assertWithObjectNode(), cache, singletonList(or(asList(equalTo("traveller.firstName", "John"), equalTo("traveller.firstName", "Jane")))), null, null, null, processInstanceId1, processInstanceId2);
        queryAndAssert(assertWithObjectNode(), cache, asList(equalTo("traveller.firstName", "John"), equalTo("traveller.lastName", "Toe")), null, null, null);

//        queryAndAssert(assertWithObjectNodeInOrder(), cache, asList(in("traveller.firstName", asList("Jane", "John")), in("traveller.lastName", asList("Doe", "Toe"))), singletonList(orderBy("traveller.lastName", SortDirection.ASC)), 1, 1, processInstanceId2);
//        queryAndAssert(assertWithObjectNodeInOrder(), cache, null, singletonList(orderBy("traveller.firstName", SortDirection.ASC)), null, null, processInstanceId2, processInstanceId1);
        queryAndAssert(assertWithObjectNodeInOrder(), cache, null, null, 1, 1, processInstanceId2);
//        queryAndAssert(assertWithObjectNodeInOrder(), cache, null, singletonList(orderBy("traveller.firstName", SortDirection.ASC)), 1, 1, processInstanceId1);
    }
}
