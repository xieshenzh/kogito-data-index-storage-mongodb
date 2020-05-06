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

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.inject.Provider;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCollection;
import io.quarkus.mongodb.panache.runtime.MongoOperations;
import org.bson.Document;
import org.kie.kogito.index.mongodb.model.DomainEntity;
import org.kie.kogito.index.mongodb.query.DomainQuery;
import org.kie.kogito.index.query.Query;

@Dependent
public class DomainCache extends AbstractCache<String, ObjectNode, Document> {

    public static String ID = "id";

    @Inject
    ProcessIdCache processIdCache;

    @Inject
    Provider<DomainQuery> domainQueryProvider;

    String processId;

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    @Override
    public MongoCollection<Document> getCollection() {
        return MongoOperations.mongoDatabase(Document.class).getCollection(this.processId + "_domain", Document.class);
    }

    @Override
    Document mapToEntity(String key, ObjectNode value) {
        return DomainEntity.fromObjectNode(key, value);
    }

    @Override
    ObjectNode mapToModel(String key, Document entity) {
        return DomainEntity.toObjectNode(key, entity);
    }

    @Override
    public Query<ObjectNode> query() {
        DomainQuery query = domainQueryProvider.get();
        query.setDomainCache(this);
        return query;
    }

    @Override
    public String getRootType() {
        return processIdCache.get(processId);
    }
}
