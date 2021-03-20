package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.function.Consumer;

/**
 * Configuration for a RedisRepository
 */
public final class RedisRepositoryConfiguration {

    private static final String DEFAULT_KEY_SEPARATOR = ":";

    private final JedisPool jedisPool;
    private final Consumer<JedisException> jedisExceptionInterceptor;
    private final String collectionKey;
    private final String keySeparator;

    private RedisRepositoryConfiguration(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {

        if (jedisPool == null) {
            throw new IllegalArgumentException("jedisPool cannot be null!");
        } else {
            this.jedisPool = jedisPool;
        }

        this.jedisExceptionInterceptor = jedisExceptionInterceptor;

        if (isNullOrEmptyOrBlank(keySeparator)) {
            this.keySeparator = DEFAULT_KEY_SEPARATOR;
        } else {
            this.keySeparator = keySeparator;
        }

        if (isNullOrEmptyOrBlank(collectionKey)) {
            throw new IllegalArgumentException("collectionKey cannot be null, nor empty!");
        } else if (collectionKey.contains(this.keySeparator)) {
            throw new IllegalArgumentException("Collection key `" + collectionKey + "` cannot contain `" + this.keySeparator + "`");
        } else {
            this.collectionKey = collectionKey;
        }
    }

    public JedisPool getJedisPool() {
        return jedisPool;
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

    private static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    /**
     * Get a builder instance for the RedisRepositoryConfiguration object.
     * @return Builder object
     */
    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {

        private JedisPool jedisPool = null;
        private Consumer<JedisException> jedisExceptionInterceptor = null;
        private String collectionKey = null;
        private String keySeparator = ":";

        /**
         * Sets a JedisPool object.
         * @param jedisPool JedisPool object
         * @return Builder object
         */
        public Builder jedisPool(final JedisPool jedisPool) {
            this.jedisPool = jedisPool;
            return this;
        }

        /**
         * Sets an interceptor for JedisException.
         * @param jedisExceptionInterceptor Consumer of a JedisException object.
         * @return Builder object
         */
        public Builder jedisExceptionInterceptor(final Consumer<JedisException> jedisExceptionInterceptor) {
            this.jedisExceptionInterceptor = jedisExceptionInterceptor;
            return this;
        }

        /**
         * Sets the collection key String object.<br/>
         * Depending of the data access pattern used, this can be the key prefix or the key itself.
         * @param collectionKey String object
         * @return Builder object
         */
        public Builder collectionKey(final String collectionKey) {
            this.collectionKey = collectionKey;
            return this;
        }

        /**
         * Sets the key separator, unless the default value is not desired.<br/>
         * Setting it is relevant only if one uses a data access pattern that uses collection keys as prefixes and not as actual keys.
         * @param keySeparator
         * @return Builder object
         */
        public Builder keySeparator(final String keySeparator) {
            this.keySeparator = keySeparator;
            return this;
        }

        /**
         * Build a RedisRepositoryConfiguration object.
         * @return RedisRepositoryConfiguration object
         */
        public RedisRepositoryConfiguration build() {
            return new RedisRepositoryConfiguration(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }
    }
}
