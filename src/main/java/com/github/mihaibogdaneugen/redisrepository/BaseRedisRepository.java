package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

abstract class BaseRedisRepository<KeyType> implements AutoCloseable {

    protected final Jedis jedis;
    protected final RedisRepositoryConfiguration configuration;

    public BaseRedisRepository(final Jedis jedis, final RedisRepositoryConfiguration configuration) {
        throwIfNull(jedis, "jedis");
        throwIfNull(configuration, "configuration");
        this.jedis = jedis;
        this.configuration = configuration;
    }

    public BaseRedisRepository(final JedisPool jedisPool, final RedisRepositoryConfiguration configuration) {
        throwIfNull(jedisPool, "jedisPool");
        throwIfNull(configuration, "configuration");
        this.jedis = jedisPool.getResource();
        this.configuration = configuration;
    }

    protected KeyType getKey(final KeyType keySuffix) {
        throw new UnsupportedOperationException();
    }

    protected KeyType getAllKeysPattern() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            jedis.close();
        } catch (final Exception exception) {
            //ignore
        }
    }

    protected static <T> void throwIfNull(final T object, final String valueName) {
        if (object == null) {
            throw new IllegalArgumentException(valueName + " cannot be null!");
        }
    }

    protected static void throwIfNullOrEmpty(final byte[] bytes, final String valueName) {
        if (isNullOrEmpty(bytes)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    protected static void throwIfNullOrEmptyOrBlank(final String value, final String valueName) {
        if (isNullOrEmptyOrBlank(value)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    protected static boolean isNullOrEmpty(final byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    protected static <T> boolean isNullOrEmpty(final Map<T, T> map) {
        return map == null || map.isEmpty();
    }

    protected static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    protected static <T> List<List<T>> getBatches(final List<T> collection, final int batchSize) {
        return IntStream.iterate(0, i -> i < collection.size(), i -> i + batchSize)
                .mapToObj(i -> collection.subList(i, Math.min(i + batchSize, collection.size())))
                .collect(Collectors.toList());
    }
}
