package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.mihaibogdaneugen.redisrepository.RedisRepositoryStrategy.*;

/**
 * Configuration for a RedisRepository
 */
public class RedisRepositoryConfiguration<T> {

    private static final String DEFAULT_KEY_SEPARATOR = ":";

    private final JedisPool jedisPool;
    private final Consumer<JedisException> jedisExceptionInterceptor;
    private final String collectionKey;
    private final String keySeparator;
    private final RedisRepositoryStrategy strategy;
    private final boolean useBinaryApi;

    private Function<T, String> serializerEachEntityIsAValue;
    private Function<String, T> deserializerEachEntityIsAValue;
    private Function<T, Map<String, String>> serializerEachEntityIsAHash;
    private Function<Map<String, String>, T> deserializerEachEntityIsAHash;
    private Function<T, String> serializerEachEntityIsAValueInAHash;
    private Function<String, T> deserializerEachEntityIsAValueInAHash;

    private Function<T, byte[]> binarySerializerEachEntityIsAValue;
    private Function<byte[], T> binaryDeserializerEachEntityIsAValue;
    private Function<T, Map<byte[], byte[]>> binarySerializerEachEntityIsAHash;
    private Function<Map<byte[], byte[]>, T> binaryDeserializerEachEntityIsAHash;
    private Function<T, byte[]> binarySerializerEachEntityIsAValueInAHash;
    private Function<byte[], T> binaryDeserializerEachEntityIsAValueInAHash;

    private RedisRepositoryConfiguration(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final RedisRepositoryStrategy strategy,
            final boolean useBinaryApi) {
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
        this.strategy = strategy;
        this.useBinaryApi = useBinaryApi;
    }

    private static <T> RedisRepositoryConfiguration<T> create(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final RedisRepositoryStrategy strategy,
            final Function<T, String> serializerEachEntityIsAValue,
            final Function<String, T> deserializerEachEntityIsAValue,
            final Function<T, Map<String, String>> serializerEachEntityIsAHash,
            final Function<Map<String, String>, T> deserializerEachEntityIsAHash,
            final Function<T, String> serializerEachEntityIsAValueInAHash,
            final Function<String, T> deserializerEachEntityIsAValueInAHash) {
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategy, false);
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (serializerEachEntityIsAValue == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE strategy the serializerEachEntityIsAValue must be non null!");
                }
                if (deserializerEachEntityIsAValue == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE strategy the deserializerEachEntityIsAValue must be non null!");
                }
                config.serializerEachEntityIsAValue = serializerEachEntityIsAValue;
                config.deserializerEachEntityIsAValue = deserializerEachEntityIsAValue;
                break;
            case EACH_ENTITY_IS_A_HASH:
                if (serializerEachEntityIsAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_HASH strategy the serializerEachEntityIsAHash must be non null!");
                }
                if (deserializerEachEntityIsAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_HASH strategy the deserializerEachEntityIsAHash must be non null!");
                }
                config.serializerEachEntityIsAHash = serializerEachEntityIsAHash;
                config.deserializerEachEntityIsAHash = deserializerEachEntityIsAHash;
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (serializerEachEntityIsAValueInAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE_IN_A_HASH strategy the serializerEachEntityIsAValueInAHash must be non null!");
                }
                if (deserializerEachEntityIsAValueInAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE_IN_A_HASH strategy the deserializerEachEntityIsAValueInAHash must be non null!");
                }
                config.serializerEachEntityIsAValueInAHash = serializerEachEntityIsAValueInAHash;
                config.deserializerEachEntityIsAValueInAHash = deserializerEachEntityIsAValueInAHash;
                break;
            default:
                throw new IllegalArgumentException("strategy cannot be NONE - a strategy must be defined!");
        }
        return config;
    }

    private static <T> RedisRepositoryConfiguration<T> createWithBinaryApi(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final RedisRepositoryStrategy strategy,
            final Function<T, byte[]> binarySerializerEachEntityIsAValue,
            final Function<byte[], T> binaryDeserializerEachEntityIsAValue,
            final Function<T, Map<byte[], byte[]>> binarySerializerEachEntityIsAHash,
            final Function<Map<byte[], byte[]>, T> binaryDeserializerEachEntityIsAHash,
            final Function<T, byte[]> binarySerializerEachEntityIsAValueInAHash,
            final Function<byte[], T> binaryDeserializerEachEntityIsAValueInAHash) {
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategy, true);
        switch (strategy) {
            case EACH_ENTITY_IS_A_VALUE:
                if (binarySerializerEachEntityIsAValue == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE strategy the binarySerializerEachEntityIsAValue must be non null!");
                }
                if (binaryDeserializerEachEntityIsAValue == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE strategy the binaryDeserializerEachEntityIsAValue must be non null!");
                }
                config.binarySerializerEachEntityIsAValue = binarySerializerEachEntityIsAValue;
                config.binaryDeserializerEachEntityIsAValue = binaryDeserializerEachEntityIsAValue;
                break;
            case EACH_ENTITY_IS_A_HASH:
                if (binarySerializerEachEntityIsAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_HASH strategy the binarySerializerEachEntityIsAHash must be non null!");
                }
                if (binaryDeserializerEachEntityIsAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_HASH strategy the binaryDeserializerEachEntityIsAHash must be non null!");
                }
                config.binarySerializerEachEntityIsAHash = binarySerializerEachEntityIsAHash;
                config.binaryDeserializerEachEntityIsAHash = binaryDeserializerEachEntityIsAHash;
                break;
            case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                if (binarySerializerEachEntityIsAValueInAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE_IN_A_HASH strategy the binarySerializerEachEntityIsAValueInAHash must be non null!");
                }
                if (binaryDeserializerEachEntityIsAValueInAHash == null) {
                    throw new IllegalArgumentException("for EACH_ENTITY_IS_A_VALUE_IN_A_HASH strategy the binaryDeserializerEachEntityIsAValueInAHash must be non null!");
                }
                config.binarySerializerEachEntityIsAValueInAHash = binarySerializerEachEntityIsAValueInAHash;
                config.binaryDeserializerEachEntityIsAValueInAHash = binaryDeserializerEachEntityIsAValueInAHash;
                break;
            default:
                throw new IllegalArgumentException("strategy cannot be NONE - a strategy must be defined!");
        }
        return config;
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

    public RedisRepositoryStrategy getStrategy() {
        return strategy;
    }

    public boolean useBinaryApi() {
        return useBinaryApi;
    }

    public String serialize(final T entity) {
        if (!useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE) {
            return serializerEachEntityIsAValue.apply(entity);
        } else if (!useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE_IN_A_HASH) {
            return serializerEachEntityIsAValueInAHash.apply(entity);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public Map<String, String> serializeToHash(final T entity) {
        if (!useBinaryApi && strategy == EACH_ENTITY_IS_A_HASH) {
            return serializerEachEntityIsAHash.apply(entity);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public byte[] binarySerialize(final T entity) {
        if (useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE) {
            return binarySerializerEachEntityIsAValue.apply(entity);
        } else if (useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE_IN_A_HASH) {
            return binarySerializerEachEntityIsAValueInAHash.apply(entity);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public Map<byte[], byte[]> binarySerializeToHash(final T entity) {
        if (useBinaryApi && strategy == EACH_ENTITY_IS_A_HASH) {
            return binarySerializerEachEntityIsAHash.apply(entity);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public T deserialize(final String value) {
        if (!useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE) {
            return deserializerEachEntityIsAValue.apply(value);
        } if (!useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE_IN_A_HASH) {
            return deserializerEachEntityIsAValueInAHash.apply(value);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public T deserializeFromHash(final Map<String, String> value) {
        if (!useBinaryApi && strategy == EACH_ENTITY_IS_A_HASH) {
            return deserializerEachEntityIsAHash.apply(value);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public T binaryDeserialize(final byte[] value) {
        if (useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE) {
            return binaryDeserializerEachEntityIsAValue.apply(value);
        } if (useBinaryApi && strategy == EACH_ENTITY_IS_A_VALUE_IN_A_HASH) {
            return binaryDeserializerEachEntityIsAValueInAHash.apply(value);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    public T binaryDeserializeFromHash(final Map<byte[], byte[]> value) {
        if (useBinaryApi && strategy == EACH_ENTITY_IS_A_HASH) {
            return binaryDeserializerEachEntityIsAHash.apply(value);
        } else {
            throw new UnsupportedOperationException("Unsupported RedisRepositoryStrategy!");
        }
    }

    private static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    /**
     * Get a builder instance for the RedisRepositoryConfiguration object.
     * @return Builder object
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static <T> BinaryApiBuilder<T> binaryApiBuilder() {
        return new BinaryApiBuilder<>();
    }

    public static class Builder<T> {

        private JedisPool jedisPool = null;
        private Consumer<JedisException> jedisExceptionInterceptor = null;
        private String collectionKey = null;
        private String keySeparator = ":";
        private RedisRepositoryStrategy strategy = RedisRepositoryStrategy.NONE;
        private Function<T, String> serializerEachEntityIsAValue = null;
        private Function<String, T> deserializerEachEntityIsAValue = null;
        private Function<T, Map<String, String>> serializerEachEntityIsAHash = null;
        private Function<Map<String, String>, T> deserializerEachEntityIsAHash = null;
        private Function<T, String> serializerEachEntityIsAValueInAHash = null;
        private Function<String, T> deserializerEachEntityIsAValueInAHash = null;

        /**
         * Sets a JedisPool object.
         * @param jedisPool JedisPool object
         * @return Builder object
         */
        public Builder<T> jedisPool(final JedisPool jedisPool) {
            this.jedisPool = jedisPool;
            return this;
        }

        /**
         * Sets an interceptor for JedisException.
         * @param jedisExceptionInterceptor Consumer of a JedisException object.
         * @return Builder object
         */
        public Builder<T> jedisExceptionInterceptor(final Consumer<JedisException> jedisExceptionInterceptor) {
            this.jedisExceptionInterceptor = jedisExceptionInterceptor;
            return this;
        }

        /**
         * Sets the collection key String object.<br/>
         * Depending of the data access pattern used, this can be the key prefix or the key itself.
         * @param collectionKey String object
         * @return Builder object
         */
        public Builder<T> collectionKey(final String collectionKey) {
            this.collectionKey = collectionKey;
            return this;
        }

        /**
         * Sets the key separator, unless the default value is not desired.<br/>
         * Setting it is relevant only if one uses a data access pattern that uses collection keys as prefixes and not as actual keys.
         * @param keySeparator String object used to separate the parts of the key
         * @return Builder object
         */
        public Builder<T> keySeparator(final String keySeparator) {
            this.keySeparator = keySeparator;
            return this;
        }

        public Builder<T> eachEntityIsAValue(final Function<T, String> serializer, final Function<String, T> deserializer) {
            this.strategy = EACH_ENTITY_IS_A_VALUE;
            this.serializerEachEntityIsAValue = serializer;
            this.deserializerEachEntityIsAValue = deserializer;
            return this;
        }

        public Builder<T> eachEntityIsAHash(final Function<T, Map<String, String>> serializer, final Function<Map<String, String>, T> deserializer) {
            this.strategy = EACH_ENTITY_IS_A_HASH;
            this.serializerEachEntityIsAHash = serializer;
            this.deserializerEachEntityIsAHash = deserializer;
            return this;
        }

        public Builder<T> eachEntityIsAValueInAHash(final Function<T, String> serializer, final Function<String, T> deserializer) {
            this.strategy = EACH_ENTITY_IS_A_VALUE_IN_A_HASH;
            this.serializerEachEntityIsAValueInAHash = serializer;
            this.deserializerEachEntityIsAValueInAHash = deserializer;
            return this;
        }

        /**
         * Build a RedisRepositoryConfiguration object.
         * @return RedisRepositoryConfiguration object
         */
        public RedisRepositoryConfiguration<T> build() {
            return create(
                    jedisPool,
                    jedisExceptionInterceptor,
                    collectionKey,
                    keySeparator,
                    strategy,
                    serializerEachEntityIsAValue,
                    deserializerEachEntityIsAValue,
                    serializerEachEntityIsAHash,
                    deserializerEachEntityIsAHash,
                    serializerEachEntityIsAValueInAHash,
                    deserializerEachEntityIsAValueInAHash
            );
        }
    }

    public static class BinaryApiBuilder<T> {

        private JedisPool jedisPool = null;
        private Consumer<JedisException> jedisExceptionInterceptor = null;
        private String collectionKey = null;
        private String keySeparator = ":";
        private RedisRepositoryStrategy strategy = RedisRepositoryStrategy.NONE;
        private Function<T, byte[]> serializerEachEntityIsAValue = null;
        private Function<byte[], T> deserializerEachEntityIsAValue = null;
        private Function<T, Map<byte[], byte[]>> serializerEachEntityIsAHash = null;
        private Function<Map<byte[], byte[]>, T> deserializerEachEntityIsAHash = null;
        private Function<T, byte[]> serializerEachEntityIsAValueInAHash = null;
        private Function<byte[], T> deserializerEachEntityIsAValueInAHash = null;

        /**
         * Sets a JedisPool object.
         * @param jedisPool JedisPool object
         * @return Builder object
         */
        public BinaryApiBuilder<T> jedisPool(final JedisPool jedisPool) {
            this.jedisPool = jedisPool;
            return this;
        }

        /**
         * Sets an interceptor for JedisException.
         * @param jedisExceptionInterceptor Consumer of a JedisException object.
         * @return Builder object
         */
        public BinaryApiBuilder<T> jedisExceptionInterceptor(final Consumer<JedisException> jedisExceptionInterceptor) {
            this.jedisExceptionInterceptor = jedisExceptionInterceptor;
            return this;
        }

        /**
         * Sets the collection key String object.<br/>
         * Depending of the data access pattern used, this can be the key prefix or the key itself.
         * @param collectionKey String object
         * @return Builder object
         */
        public BinaryApiBuilder<T> collectionKey(final String collectionKey) {
            this.collectionKey = collectionKey;
            return this;
        }

        /**
         * Sets the key separator, unless the default value is not desired.<br/>
         * Setting it is relevant only if one uses a data access pattern that uses collection keys as prefixes and not as actual keys.
         * @param keySeparator String object used to separate the parts of the key
         * @return Builder object
         */
        public BinaryApiBuilder<T> keySeparator(final String keySeparator) {
            this.keySeparator = keySeparator;
            return this;
        }

        public BinaryApiBuilder<T> eachEntityIsAValue(final Function<T, byte[]> serializer, final Function<byte[], T> deserializer) {
            this.strategy = EACH_ENTITY_IS_A_VALUE;
            this.serializerEachEntityIsAValue = serializer;
            this.deserializerEachEntityIsAValue = deserializer;
            return this;
        }

        public BinaryApiBuilder<T> eachEntityIsAHash(final Function<T, Map<byte[], byte[]>> serializer, final Function<Map<byte[], byte[]>, T> deserializer) {
            this.strategy = EACH_ENTITY_IS_A_HASH;
            this.serializerEachEntityIsAHash = serializer;
            this.deserializerEachEntityIsAHash = deserializer;
            return this;
        }

        public BinaryApiBuilder<T> eachEntityIsAValueInAHash(final Function<T, byte[]> serializer, final Function<byte[], T> deserializer) {
            this.strategy = EACH_ENTITY_IS_A_VALUE_IN_A_HASH;
            this.serializerEachEntityIsAValueInAHash = serializer;
            this.deserializerEachEntityIsAValueInAHash = deserializer;
            return this;
        }

        /**
         * Build a RedisRepositoryConfiguration object.
         * @return RedisRepositoryConfiguration object
         */
        public RedisRepositoryConfiguration<T> build() {
            return createWithBinaryApi(
                    jedisPool,
                    jedisExceptionInterceptor,
                    collectionKey,
                    keySeparator,
                    strategy,
                    serializerEachEntityIsAValue,
                    deserializerEachEntityIsAValue,
                    serializerEachEntityIsAHash,
                    deserializerEachEntityIsAHash,
                    serializerEachEntityIsAValueInAHash,
                    deserializerEachEntityIsAValueInAHash
            );
        }
    }
}
