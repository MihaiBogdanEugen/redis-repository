package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class StringRedisRepository<T> extends BaseRedisRepository<T> {

    private final String keyPrefix;
    private final String allKeysPattern;

    public StringRedisRepository(final Jedis jedis, final String collectionKey) {
        super(jedis);
        throwIfNullOrEmptyOrBlank(collectionKey, "collectionKey");
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = collectionKey + DEFAULT_KEY_SEPARATOR + "*";
    }

    public StringRedisRepository(final JedisPool jedisPool, final String collectionKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(collectionKey, "collectionKey");
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = collectionKey + DEFAULT_KEY_SEPARATOR + "*";
    }

    public abstract String convertTo(final T entity);

    public abstract T convertFrom(final String entity);

    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        final var entity = jedis.get(key);
        return isNullOrEmptyOrBlank(entity)
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
        jedis.set(key, convertTo(entity));
    }

    @Override
    public final void setIfNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        jedis.setnx(key, convertTo(entity));
    }

    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        final var key = getKey(id);
        jedis.watch(key);
        final var value = jedis.get(key);
        if (isNullOrEmptyOrBlank(value)) {
            jedis.unwatch();
            return Optional.empty();
        }
        final var entity = convertFrom(value);
        final var newEntity = updater.apply(entity);
        final var newValue = convertTo(newEntity);
        final List<Object> results;
        try (final var transaction = jedis.multi()) {
            transaction.set(key, newValue);
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

    public final void set(final String id, final T entity, final SetParams setParams) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        throwIfNull(setParams, "setParams");
        final var key = getKey(id);
        jedis.set(key, convertTo(entity), setParams);
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

    private String getKey(final String keySuffix) {
        return keyPrefix + keySuffix;
    }

    private String[] getKeys(final String... keySuffixes) {
        return Arrays.stream(keySuffixes)
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::getKey)
                .toArray(String[]::new);
    }

    private List<T> getByKeys(final String... keys) {
        final var values = jedis.mget(keys);
        return values.stream()
                .filter(BaseRedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    private String[] getAllKeys() {
        return jedis.keys(allKeysPattern).toArray(String[]::new);
    }
}
