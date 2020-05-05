package org.kie.kogito.index.mongodb.query;

import java.util.List;

import javax.inject.Inject;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.kogito.index.cache.Cache;
import org.kie.kogito.index.cache.CacheService;
import org.kie.kogito.index.mongodb.MongoDBServerTestResource;
import org.kie.kogito.index.query.AttributeFilter;
import org.kie.kogito.index.query.AttributeSort;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.kie.kogito.index.query.QueryFilterFactory.and;
import static org.kie.kogito.index.query.QueryFilterFactory.contains;
import static org.kie.kogito.index.query.QueryFilterFactory.containsAll;
import static org.kie.kogito.index.query.QueryFilterFactory.containsAny;
import static org.kie.kogito.index.query.QueryFilterFactory.equalTo;
import static org.kie.kogito.index.query.QueryFilterFactory.in;
import static org.kie.kogito.index.query.QueryFilterFactory.like;
import static org.kie.kogito.index.query.QueryFilterFactory.notNull;
import static org.kie.kogito.index.query.QueryFilterFactory.or;

@QuarkusTest
@QuarkusTestResource(MongoDBServerTestResource.class)
public class ProcessIdQueryTest {

    @Inject
    CacheService cacheService;

    Cache<String, String> cache;

    @BeforeEach
    void setUp() {
        this.cache = cacheService.getProcessIdModelCache();
    }

    @AfterEach
    void tearDown() {
        cache.clear();
        Assert.assertTrue(cache.isEmpty());
    }

    @Test
    void test() {
        String processId = "travels";
        String subProcessId = "travels_sub";
        String type1 = "org.acme.travels.travels";
        String type2 = "org.acme.travels";
        cache.put(processId, type1);
        cache.put(subProcessId, type2);

        queryAndAssert(singletonList(in("processId", asList(processId, subProcessId))), null, null, null, type1, type2);
        queryAndAssert(singletonList(equalTo("processId", processId)), null, null, null, type1);
        queryAndAssert(singletonList(notNull("processId")), null, null, null, type1, type2);
        queryAndAssert(singletonList(contains("fullTypeName", type1)), null, null, null, type1);
        queryAndAssert(singletonList(containsAny("fullTypeName", asList(type1, type2))), null, null, null, type1, type2);
        queryAndAssert(singletonList(containsAll("processId", asList(processId, subProcessId))), null, null, null);
        queryAndAssert(singletonList(like("processId", "*_sub")), null, null, null, type2);
        queryAndAssert(singletonList(and(asList(equalTo("processId", processId), equalTo("fullTypeName", type1)))), null, null, null, type1);
        queryAndAssert(singletonList(or(asList(equalTo("processId", processId), equalTo("fullTypeName", type2)))), null, null, null, type1, type2);
        queryAndAssert(asList(equalTo("processId", processId), equalTo("fullTypeName", type2)), null, null, null);

//        queryAndAssert(singletonList(in("processId", asList(processId, subProcessId))), singletonList(orderBy("fullTypeName", SortDirection.DESC)), 1, 1, type2);
//        queryAndAssert(null, singletonList(orderBy("fullTypeName", SortDirection.DESC)), 1, 1, type2);
//        queryAndAssert(null, singletonList(orderBy("fullTypeName", SortDirection.DESC)), null, null, type2, type1);
        queryAndAssert(null, null, 1, 1, type2);
    }

    private void queryAndAssert(List<AttributeFilter<?>> filters, List<AttributeSort> sort, Integer offset, Integer limit, String... ids) {
        List<String> instances = cache.query().filter(filters).sort(sort).offset(offset).limit(limit).execute();
        assertThat(instances).hasSize(ids == null ? 0 : ids.length).containsExactly(ids);
    }
}
