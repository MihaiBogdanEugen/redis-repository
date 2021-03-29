package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.exceptions.JedisException;

import java.util.function.Consumer;

public final class ClusterRedisRepositoryConfiguration {

    private static final String DEFAULT_KEY_SEPARATOR = ":";
    private static final int DEFAULT_MAX_ATTEMPTS = 5;

    private final JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler;
    private final Consumer<JedisException> jedisExceptionInterceptor;
    private final String collectionKey;
    private final String keySeparator;
    private final int maxAttempts;

    private ClusterRedisRepositoryConfiguration(final JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator, final int maxAttempts) {

        if (jedisSlotBasedConnectionHandler == null) {
            throw new IllegalArgumentException("jedisSlotBasedConnectionHandler cannot be null!");
        } else {
            this.jedisSlotBasedConnectionHandler = jedisSlotBasedConnectionHandler;
        }

        this.jedisExceptionInterceptor = jedisExceptionInterceptor;
        this.keySeparator = isNullOrEmptyOrBlank(keySeparator) ? DEFAULT_KEY_SEPARATOR : keySeparator;

        if (isNullOrEmptyOrBlank(collectionKey)) {
            throw new IllegalArgumentException("collectionKey cannot be null nor empty!");
        } else if (collectionKey.contains(this.keySeparator)) {
            throw new IllegalArgumentException("Collection key `" + collectionKey + "` cannot contain `" + this.keySeparator + "`");
        } else {
            this.collectionKey = collectionKey;
        }

        this.maxAttempts = maxAttempts < 1 ? DEFAULT_MAX_ATTEMPTS : maxAttempts;
    }

    public JedisSlotBasedConnectionHandler getJedisSlotBasedConnectionHandler() {
        return jedisSlotBasedConnectionHandler;
    }

    public Consumer<JedisException> getJedisExceptionInterceptor() {
        return jedisExceptionInterceptor;
    }

    public String getCollectionKey() {
        return collectionKey;
    }

    public String getKeySeparator() {
        return keySeparator;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    private static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {

        private JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler = null;
        private Consumer<JedisException> jedisExceptionInterceptor = null;
        private String collectionKey = null;
        private String keySeparator = null;
        private int maxAttempts = 0;

        public Builder jedisPool(final JedisSlotBasedConnectionHandler jedisSlotBasedConnectionHandler) {
            this.jedisSlotBasedConnectionHandler = jedisSlotBasedConnectionHandler;
            return this;
        }

        public Builder jedisExceptionInterceptor(final Consumer<JedisException> jedisExceptionInterceptor) {
            this.jedisExceptionInterceptor = jedisExceptionInterceptor;
            return this;
        }

        public Builder collectionKey(final String collectionKey) {
            this.collectionKey = collectionKey;
            return this;
        }

        public Builder keySeparator(final String keySeparator) {
            this.keySeparator = keySeparator;
            return this;
        }

        public Builder maxAttempts(final int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public ClusterRedisRepositoryConfiguration build() {
            return new ClusterRedisRepositoryConfiguration(jedisSlotBasedConnectionHandler, jedisExceptionInterceptor, collectionKey, keySeparator, maxAttempts);
        }
    }
}
