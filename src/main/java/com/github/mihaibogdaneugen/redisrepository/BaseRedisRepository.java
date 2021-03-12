package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

abstract class BaseRedisRepository<T> implements RedisRepository<T>, AutoCloseable {

    protected static final String DEFAULT_KEY_SEPARATOR = ":";

    protected final Jedis jedis;

    public BaseRedisRepository(final Jedis jedis) {
        throwIfNull(jedis, "jedis");
        this.jedis = jedis;
    }

    public BaseRedisRepository(final JedisPool jedisPool) {
        throwIfNull(jedisPool, "jedisPool");
        this.jedis = jedisPool.getResource();
    }

    @Override
    public final void close() {
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

    protected static void throwIfNullOrEmpty(final String[] strings) {
        if (isNullOrEmpty(strings)) {
            throw new IllegalArgumentException("ids cannot be null, nor empty!");
        }
    }

    protected static void throwIfNullOrEmptyOrBlank(final String value, final String valueName) {
        if (isNullOrEmptyOrBlank(value)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    protected static void throwIfNegative(final long value, final String valueName) {
        if (value < 0) {
            throw new IllegalArgumentException(valueName + " cannot have a negative value!");
        }
    }

    protected static boolean isNullOrEmpty(final byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    protected static boolean isNullOrEmpty(final String[] strings) {
        return strings == null || strings.length == 0;
    }

    protected static <T> boolean isNotNullNorEmpty(final List<T> list) {
        return list != null && !list.isEmpty();
    }

    protected static <T> boolean isNotNullNorEmpty(final Map<T, T> map) {
        return map != null && !map.isEmpty();
    }

    protected static <T> boolean isNullOrEmpty(final Map<T, T> map) {
        return map == null || map.isEmpty();
    }

    protected static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    protected static boolean isNotNullNorEmptyNorBlank(final String text) {
        return !isNullOrEmptyOrBlank(text);
    }

    protected static boolean isNotNullNorEmpty(final byte[] bytes) {
        return !isNullOrEmpty(bytes);
    }
}
