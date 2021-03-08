package com.github.mihaibogdaneugen.redisrepository;

import java.util.Optional;

public interface BinaryRepository<T extends RedisEntity<byte[]>> {

    Optional<T> get(final byte[] id);

    Boolean exists(final byte[] id);

    void set(final T entity);
}
