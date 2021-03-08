package com.github.mihaibogdaneugen.redisrepository;

import java.util.Optional;

public interface StringRepository<T extends RedisEntity<String>> {

    Optional<T> get(final String id);

    Boolean exists(final String id);

    void set(final T entity);
}
