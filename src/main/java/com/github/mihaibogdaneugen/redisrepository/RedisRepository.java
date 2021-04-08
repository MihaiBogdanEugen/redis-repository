package com.github.mihaibogdaneugen.redisrepository;

import java.util.Set;

public interface RedisRepository<T> extends RedisClusterRepository<T> {

    /**
     * Retrieve all keys of all entities in the current collection.
     * @return Set of String objects representing entity identifiers
     */
    Set<String> getAllIds();
}
