package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.util.SafeEncoder;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class BinaryHashRedisRepository<T> extends BaseRedisRepository<T> {

    private final String keyPrefix;
    private final byte[] allKeysPattern;

    public BinaryHashRedisRepository(final Jedis jedis, final String collectionKey) {
        super(jedis);
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = SafeEncoder.encode(collectionKey + DEFAULT_KEY_SEPARATOR + "*");
    }

    public BinaryHashRedisRepository(final JedisPool jedisPool, final String collectionKey) {
        super(jedisPool);
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = SafeEncoder.encode(collectionKey + DEFAULT_KEY_SEPARATOR + "*");
    }

    public abstract Map<byte[], byte[]> convertTo(final T entity);

    public abstract T convertFrom(final Map<byte[], byte[]> entity);

    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        final var entity = jedis.hgetAll(key);
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    @Override
    public final List<T> get(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var keys = getKeys(ids);
        return getByKeys(keys);
    }

    @Override
    public final List<T> getAll() {
        final var keys = getAllKeys();
        return getByKeys(keys);
    }

    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return jedis.exists(key);
    }

    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        jedis.hset(key, convertTo(entity));
    }

    @Override
    public final void setIfNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        jedis.watch(key);
        if (jedis.exists(key)) {
            jedis.unwatch();
            return;
        }
        try (final var transaction = jedis.multi()) {
            transaction.hset(key, convertTo(entity));
            transaction.exec();
        }
    }

    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        final var key = getKey(id);
        jedis.watch(key);
        final var value = jedis.hgetAll(key);
        if (isNullOrEmpty(value)) {
            jedis.unwatch();
            return Optional.empty();
        }
        final var entity = convertFrom(value);
        final var newEntity = updater.apply(entity);
        final var newValue = convertTo(newEntity);
        final List<Object> results;
        try (final var transaction = jedis.multi()) {
            transaction.hset(key, newValue);
            results = transaction.exec();
        }
        return Optional.of(isNotNullNorEmpty(results));
    }

    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        jedis.del(key);
    }

    @Override
    public final void delete(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var keys = getKeys(ids);
        jedis.del(keys);
    }

    @Override
    public final void deleteAll() {
        final var keys = getAllKeys();
        jedis.del(keys);
    }

    public final void setExpirationAfter(final String id, final long milliseconds) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNegative(milliseconds, "milliseconds");
        final var key = getKey(id);
        jedis.pexpire(key, milliseconds);
    }

    public final void setExpirationAt(final String id, final long millisecondsTimestamp) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNegative(millisecondsTimestamp, "millisecondsTimestamp");
        final var key = getKey(id);
        jedis.pexpireAt(key, millisecondsTimestamp);
    }

    public final Long getTimeToLiveLeft(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return jedis.pttl(key);
    }

    private List<T> getByKeys(final byte[]... keys) {
        final var responses = new ArrayList<Response<Map<byte[], byte[]>>>();
        try (final var pipeline = jedis.pipelined()) {
            Arrays.stream(keys).forEach(key -> responses.add(pipeline.hgetAll(key)));
            pipeline.sync();
        }
        return responses.stream()
                .map(response -> convertFrom(response.get()))
                .collect(Collectors.toList());
    }

    private byte[][] getKeys(final String... keySuffixes) {
        return Arrays.stream(keySuffixes)
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::getKey)
                .toArray(byte[][]::new);
    }

    private byte[] getKey(final String keySuffix) {
        return SafeEncoder.encode(keyPrefix + keySuffix);
    }

    private byte[][] getAllKeys() {
        return jedis.keys(allKeysPattern).toArray(byte[][]::new);
    }
}