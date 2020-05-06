package org.kie.kogito.index.mongodb.model;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.mongodb.panache.runtime.MongoOperations;
import org.bson.BsonString;
import org.bson.Document;
import org.kie.kogito.index.mongodb.cache.DomainCache;
import org.kie.kogito.index.mongodb.utils.ModelUtils;

import static org.kie.kogito.index.mongodb.utils.ModelUtils.jsonNodeToDocument;

public class DomainEntity {

    public static ObjectNode toObjectNode(String id, Document document) {
        if (document == null) {
            return null;
        }

        ObjectNode node = ModelUtils.documentToJsonNode(document, ObjectNode.class);
        node.remove(MongoOperations.ID);
        node.put(DomainCache.ID, id);
        return node;
    }

    public static Document fromObjectNode(String id, ObjectNode node) {
        if (node == null) {
            return null;
        }

        ObjectNode n = node.deepCopy();
        n.remove(DomainCache.ID);
        return jsonNodeToDocument(n).append(MongoOperations.ID, new BsonString(id));
    }
}
