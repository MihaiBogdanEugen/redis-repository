package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class RedisRepository implements AutoCloseable {

    protected static final String DEFAULT_KEY_SEPARATOR = ":";

    protected final JedisPool jedisPool;
    protected final Consumer<JedisException> jedisExceptionInterceptor;

    public RedisRepository(final JedisPool jedisPool) {
        throwIfNull(jedisPool, "jedisPool");
        this.jedisPool = jedisPool;
        this.jedisExceptionInterceptor = null;
    }

    public RedisRepository(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor) {
        throwIfNull(jedisPool, "jedisPool");
        throwIfNull(jedisExceptionInterceptor, "jedisExceptionInterceptor");
        this.jedisPool = jedisPool;
        this.jedisExceptionInterceptor = jedisExceptionInterceptor;
    }

    @Override
    public void close() {
        this.jedisPool.close();
    }

    protected <T> T getResult(final Function<Jedis, T> operation) {
        throwIfNull(operation, "operation");
        try (final var jedis = jedisPool.getResource()) {
            return operation.apply(jedis);
        } catch (final JedisException exception) {
            if (jedisExceptionInterceptor != null) {
                jedisExceptionInterceptor.accept(exception);
            }
            throw exception;
        }
    }

    protected void execute(final Consumer<Jedis> operation) {
        throwIfNull(operation, "operation");
        try (final var jedis = jedisPool.getResource()) {
            operation.accept(jedis);
        } catch (final JedisException exception) {
            if (jedisExceptionInterceptor != null) {
                jedisExceptionInterceptor.accept(exception);
            }
            throw exception;
        }
    }

    /**
     * Runs a given Lua script.
     * @param script The Lua script
     * @param keys The list of String keys
     * @param args The list of String arguments
     */
    public final void eval(final String script, final List<String> keys, final List<String> args) {
        throwIfNullOrEmptyOrBlank(script, "script");
        throwIfNullOrEmpty(keys, "keys");
        throwIfNullOrEmpty(args, "args");
        execute(jedis -> jedis.eval(script, keys, args));
    }

    /**
     * Runs a given Lua script.
     * @param script The Lua script
     */
    public final void eval(final String script) {
        throwIfNullOrEmptyOrBlank(script, "script");
        execute(jedis -> jedis.eval(script));
    }

    /**
     * Runs a given Lua script expressed in a binary form.
     * @param script The Lua script in a binary form
     * @param keys The list of binary keys
     * @param args The list of binary arguments
     */
    public final void eval(final byte[] script, final List<byte[]> keys, final List<byte[]> args) {
        throwIfNullOrEmpty(script);
        throwIfNullOrEmpty(keys, "keys");
        throwIfNullOrEmpty(args, "args");
        execute(jedis -> jedis.eval(script, keys, args));
    }

    /**
     * Runs a given Lua script expressed in a binary form.
     * @param script The Lua script in a binary form
     */
    public final void eval(final byte[] script) {
        throwIfNullOrEmpty(script);
        execute(jedis -> jedis.eval(script));
    }

    protected static <T> void throwIfNull(final T object, final String valueName) {
        if (object == null) {
            throw new IllegalArgumentException(valueName + " cannot be null!");
        }
    }

    protected static void throwIfNullOrEmpty(final byte[] bytes) {
        if (isNullOrEmpty(bytes)) {
            throw new IllegalArgumentException("script cannot be null, nor empty!");
        }
    }

    protected static <T> void throwIfNullOrEmpty(final Collection<T> strings, final String valueName) {
        if (isNullOrEmpty(strings)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
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

    protected static <T> boolean isNullOrEmpty(final T[] strings) {
        return strings == null || strings.length == 0;
    }

    protected static <T> boolean isNullOrEmpty(final Collection<T> collection) {
        return collection == null || collection.isEmpty();
    }

    protected static <T> boolean isNotNullNorEmpty(final Map<T, T> map) {
        return !isNullOrEmpty(map);
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
