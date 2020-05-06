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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.quarkus.mongodb.panache.runtime.MongoOperations;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.kie.kogito.index.cache.Cache;

public abstract class AbstractCache<K, V, E> implements Cache<K, V> {

    Optional<Consumer<V>> objectCreatedListener = Optional.empty();
    Optional<Consumer<V>> objectUpdatedListener = Optional.empty();
    Optional<Consumer<K>> objectRemovedListener = Optional.empty();

    @Override
    public void addObjectCreatedListener(Consumer<V> consumer) {
        this.objectCreatedListener = Optional.ofNullable(consumer);
    }

    @Override
    public void addObjectUpdatedListener(Consumer<V> consumer) {
        this.objectUpdatedListener = Optional.ofNullable(consumer);
    }

    @Override
    public void addObjectRemovedListener(Consumer<K> consumer) {
        this.objectRemovedListener = Optional.ofNullable(consumer);
    }

    @Override
    public boolean containsKey(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K k, V v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o, Object o1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K k, V v, V v1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K k, V v) {
        throw new UnsupportedOperationException();
    }

    abstract MongoCollection<E> getCollection();

    abstract E mapToEntity(K key, V value);

    abstract V mapToModel(K key, E entity);

    @Override
    public V get(Object o) {
        return Optional.ofNullable(getCollection().find(new Document(MongoOperations.ID, o)).first()).map(e -> mapToModel((K) o, e)).orElse(null);
    }

    @Override
    public V put(K s, V v) {
        V oldValue = this.get(s);
        Optional.ofNullable(v).map(n -> mapToEntity(s, n)).ifPresent(
                e -> getCollection().replaceOne(
                        new BsonDocument(MongoOperations.ID, new BsonString(s.toString())),
                        e, new ReplaceOptions().upsert(true)));
        Optional.ofNullable(oldValue).map(o -> this.objectUpdatedListener).orElseGet(() -> this.objectCreatedListener).ifPresent(l -> l.accept(v));
        return oldValue;
    }

    @Override
    public void clear() {
        getCollection().deleteMany(new Document());
    }

    @Override
    public V remove(Object o) {
        V oldValue = this.get(o);
        Optional.ofNullable(oldValue).ifPresent(i -> getCollection().deleteOne(new Document(MongoOperations.ID, o)));
        Optional.ofNullable(oldValue).flatMap(i -> this.objectRemovedListener).ifPresent(l -> l.accept((K) o));
        return oldValue;
    }

    @Override
    public int size() {
        return (int) getCollection().countDocuments();
    }

    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }
}
