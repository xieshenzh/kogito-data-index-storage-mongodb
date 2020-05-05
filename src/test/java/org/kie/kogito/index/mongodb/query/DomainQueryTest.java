package org.kie.kogito.index.mongodb.query;

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

    }
}
