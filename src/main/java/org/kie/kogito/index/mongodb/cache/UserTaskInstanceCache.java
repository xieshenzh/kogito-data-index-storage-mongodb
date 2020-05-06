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

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Provider;

import com.mongodb.client.MongoCollection;
import io.quarkus.mongodb.panache.runtime.MongoOperations;
import org.kie.kogito.index.model.UserTaskInstance;
import org.kie.kogito.index.mongodb.model.UserTaskInstanceEntity;
import org.kie.kogito.index.mongodb.query.UserTaskInstanceQuery;
import org.kie.kogito.index.query.Query;

@ApplicationScoped
public class UserTaskInstanceCache extends AbstractCache<String, UserTaskInstance, UserTaskInstanceEntity> {

    @Inject
    Provider<UserTaskInstanceQuery> userTaskInstanceQueryProvider;

    @Override
    public MongoCollection<UserTaskInstanceEntity> getCollection() {
        return MongoOperations.mongoCollection(UserTaskInstanceEntity.class);
    }

    @Override
    UserTaskInstanceEntity mapToEntity(String key, UserTaskInstance value) {
        return UserTaskInstanceEntity.fromUserTaskInstance(value);
    }

    @Override
    UserTaskInstance mapToModel(String key, UserTaskInstanceEntity entity) {
        return UserTaskInstanceEntity.toUserTaskInstance(entity);
    }

    @Override
    public Query<UserTaskInstance> query() {
        return userTaskInstanceQueryProvider.get();
    }

    @Override
    public String getRootType() {
        return UserTaskInstance.class.getName();
    }
}
