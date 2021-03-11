package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class FieldStringHashRedisRepository<T> extends BaseRedisRepository<T> {

    private static final String DEFAULT_LOCK_KEY = "locks";

    private final String parentKey;

    public FieldStringHashRedisRepository(final Jedis jedis, final String parentKey) {
        super(jedis);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        this.parentKey = parentKey;
    }

    public FieldStringHashRedisRepository(final JedisPool jedisPool, final String parentKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        this.parentKey = parentKey;
    }

    public abstract String convertTo(final T entity);

    public abstract T convertFrom(final String entity);

    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var entity = jedis.hget(parentKey, id);
        return isNullOrEmptyOrBlank(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final List<T> get(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var entities = jedis.hmget(parentKey, ids);
        return entities.stream()
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    @Override
    public final List<T> getAll() {
        return jedis.hgetAll(parentKey).values().stream()
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        return jedis.hexists(parentKey, id);
    }

    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        jedis.hset(parentKey, id, convertTo(entity));
    }

    @Override
    public final void setIfNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        jedis.hsetnx(parentKey, id, convertTo(entity));
    }

    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        final var lockKey = getLockKey(id);
        jedis.watch(lockKey);
        final var value = jedis.hget(parentKey, id);
        if (isNullOrEmptyOrBlank(value)) {
            jedis.unwatch();
            return Optional.empty();
        }
        final var entity = convertFrom(value);
        final var newEntity = updater.apply(entity);
        final var newValue = convertTo(newEntity);
        final List<Object> results;
        try (final var transaction = jedis.multi()) {
            transaction.set(lockKey, UUID.randomUUID().toString());
            transaction.hset(parentKey, id, newValue);
            results = transaction.exec();
        }
        return Optional.of(isNotNullNorEmpty(results));
    }

    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        jedis.hdel(parentKey, id);
    }

    @Override
    public final void delete(final String... ids) {
        throwIfNullOrEmpty(ids);
        jedis.hdel(parentKey, ids);
    }

    @Override
    public final void deleteAll() {
        jedis.del(parentKey);
    }

    private String getLockKey(final String id) {
        return DEFAULT_LOCK_KEY + DEFAULT_KEY_SEPARATOR + parentKey + DEFAULT_KEY_SEPARATOR + id;
    }
}
