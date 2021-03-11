package com.github.mihaibogdaneugen.redisrepository;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public interface RedisRepository<T> {

    Optional<T> get(final String id);

    List<T> get(final String... ids);

    List<T> getAll();

    Boolean exists(final String id);

    void set(final String id, final T entity);

    void setIfNotExist(final String id, final T entity);

    Optional<Boolean> update(final String id, final Function<T, T> updater);

    void delete(final String id);

    void delete(final String... ids);

    void deleteAll();
}
