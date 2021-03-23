package com.github.mihaibogdaneugen.redisrepository.cluster;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClusterCommand;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class ClusterRedisRepository {

    private final JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler;
    private final Consumer<JedisException> jedisExceptionInterceptor;
    private final int maxAttempts;

    public ClusterRedisRepository(final JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler, final Consumer<JedisException> jedisExceptionInterceptor, final int maxAttempts) {
        throwIfNull(jedisSlotBasedConnectionHandler, "jedisSlotBasedConnectionHandler");
        throwIfInvalidValue(maxAttempts, "maxAttempts");
        this.jedisSlotBasedConnectionHandler = jedisSlotBasedConnectionHandler;
        this.jedisExceptionInterceptor = jedisExceptionInterceptor;
        this.maxAttempts = maxAttempts;
    }

    protected <T> T runClusterCommand(final String key, final Function<Jedis, T> operation) {
        throwIfNullOrEmptyOrBlank(key, "key");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<T>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public T execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.run(key);
    }

    protected <T> T runBinaryClusterCommand(final byte[] key, final Function<Jedis, T> operation) {
        throwIfNullOrEmpty(key, "key");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<T>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public T execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.runBinary(key);
    }


    protected <T> T runClusterCommand(final Set<String> keys, final Function<Jedis, T> operation) {
        throwIfNullOrEmpty(keys, "keys");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<T>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public T execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.run(keys.size(), keys.toArray(String[]::new));
    }

    protected <T> T runBinaryClusterCommand(final Set<byte[]> keys, final Function<Jedis, T> operation) {
        throwIfNullOrEmpty(keys, "keys");
        throwIfNull(operation, "operation");
        return new JedisClusterCommand<T>(jedisSlotBasedConnectionHandler, maxAttempts) {
            @Override
            public T execute(final Jedis connection) {
                try {
                    return operation.apply(connection);
                } catch (final JedisException exception) {
                    if (jedisExceptionInterceptor != null) {
                        jedisExceptionInterceptor.accept(exception);
                    }
                    throw exception;
                }
            }
        }.runBinary(keys.size(), keys.toArray(byte[][]::new));
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

    protected static <T> void throwIfNullOrEmpty(final Collection<T> strings, final String valueName) {
        if (isNullOrEmpty(strings)) {
            throw new IllegalArgumentException(valueName + " cannot be null, nor empty!");
        }
    }

    protected static void throwIfInvalidValue(int value, final String valueName) {
        if (value < 1) {
            throw new IllegalArgumentException(valueName + " cannot be less than 1!");
        }
    }

    protected static void throwIfNegative(long value, final String valueName) {
        if (value < 0) {
            throw new IllegalArgumentException(valueName + " cannot be less than 1!");
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

    protected static <T> boolean isNullOrEmpty(final T[] strings) {
        return strings == null || strings.length == 0;
    }

    protected static <T> boolean isNullOrEmpty(final Collection<T> collection) {
        return collection == null || collection.isEmpty();
    }

    protected static <T> boolean isNullOrEmpty(final Map<T, T> map) {
        return map == null || map.isEmpty();
    }

    protected static boolean isNotNullNorEmpty(final byte[] bytes) {
        return !isNullOrEmpty(bytes);
    }

    protected static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    protected static boolean isNotNullNorEmptyNorBlank(final String text) {
        return !isNullOrEmptyOrBlank(text);
    }
}
