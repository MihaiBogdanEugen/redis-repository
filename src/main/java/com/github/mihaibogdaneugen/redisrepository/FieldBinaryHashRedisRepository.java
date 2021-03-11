package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class FieldBinaryHashRedisRepository<T> extends BaseRedisRepository<T> {

    private static final String DEFAULT_LOCK_KEY = "locks";

    private final byte[] parentKey;

    public FieldBinaryHashRedisRepository(final Jedis jedis, final String parentKey) {
        super(jedis);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        this.parentKey = SafeEncoder.encode(parentKey);
    }

    public FieldBinaryHashRedisRepository(final JedisPool jedisPool, final String parentKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        this.parentKey = SafeEncoder.encode(parentKey);
    }

    public abstract byte[] convertTo(final T entity);

    public abstract T convertFrom(final byte[] entity);

    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var entity = jedis.hget(parentKey, getKey(id));
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final List<T> get(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var entities = jedis.hmget(parentKey, getKeys(ids));
        return entities.stream()
                .filter(BaseRedisRepository::isNotNullNorEmpty)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    @Override
    public final List<T> getAll() {
        return jedis.hgetAll(parentKey).values().stream()
                .filter(BaseRedisRepository::isNotNullNorEmpty)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        return jedis.hexists(parentKey, getKey(id));
    }

    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        jedis.hset(parentKey, getKey(id), convertTo(entity));
    }

    @Override
    public final void setIfNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        jedis.hsetnx(parentKey, getKey(id), convertTo(entity));
    }

    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        final var lockKey = getLockKey(id);
        jedis.watch(lockKey);
        final var value = jedis.hget(parentKey, getKey(id));
        if (isNullOrEmpty(value)) {
            jedis.unwatch();
            return Optional.empty();
        }
        final var entity = convertFrom(value);
        final var newEntity = updater.apply(entity);
        final var newValue = convertTo(newEntity);
        final List<Object> results;
        try (final var transaction = jedis.multi()) {
            transaction.set(lockKey, SafeEncoder.encode(UUID.randomUUID().toString()));
            transaction.hset(parentKey, getKey(id), newValue);
            results = transaction.exec();
        }
        return Optional.of(isNotNullNorEmpty(results));
    }

    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        jedis.hdel(parentKey, getKey(id));
    }

    @Override
    public final void delete(final String... ids) {
        throwIfNullOrEmpty(ids);
        jedis.hdel(parentKey, getKeys(ids));
    }

    @Override
    public final void deleteAll() {
        jedis.del(parentKey);
    }

    private byte[] getKey(final String id) {
        return SafeEncoder.encode(id);
    }

    private byte[][] getKeys(final String... ids) {
        return Arrays.stream(ids)
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::getKey)
                .toArray(byte[][]::new);
    }

    private byte[] getLockKey(final String id) {
        return SafeEncoder.encode(DEFAULT_LOCK_KEY + DEFAULT_KEY_SEPARATOR + SafeEncoder.encode(parentKey) + DEFAULT_KEY_SEPARATOR + id);
    }
}
