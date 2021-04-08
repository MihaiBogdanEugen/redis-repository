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
public final class RedisRepositoryConfiguration<T> {

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
        throwIfNull(jedisPool, "jedisPool");
        this.jedisPool = jedisPool;
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
        if (strategy == NONE) {
            throw new IllegalArgumentException("invalid RedisRepositoryStrategy");
        }
        this.strategy = strategy;
        this.useBinaryApi = useBinaryApi;
    }

    private static <T> RedisRepositoryConfiguration<T> createEachEntityIsAValue(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final Function<T, String> serializer,
            final Function<String, T> deserializer) {
        throwIfNull(serializer, "serializer");
        throwIfNull(deserializer, "deserializer");
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, EACH_ENTITY_IS_A_VALUE, false);
        config.serializerEachEntityIsAValue = serializer;
        config.deserializerEachEntityIsAValue = deserializer;
        return config;
    }

    private static <T> RedisRepositoryConfiguration<T> createEachEntityIsAHash(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final Function<T, Map<String, String>> serializer,
            final Function<Map<String, String>, T> deserializer) {
        throwIfNull(serializer, "serializer");
        throwIfNull(deserializer, "deserializer");
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, EACH_ENTITY_IS_A_HASH, false);
        config.serializerEachEntityIsAHash = serializer;
        config.deserializerEachEntityIsAHash = deserializer;
        return config;
    }

    private static <T> RedisRepositoryConfiguration<T> createEachEntityIsAValueInAHash(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final Function<T, String> serializer,
            final Function<String, T> deserializer) {
        throwIfNull(serializer, "serializer");
        throwIfNull(deserializer, "deserializer");
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, EACH_ENTITY_IS_A_VALUE_IN_A_HASH, false);
        config.serializerEachEntityIsAValueInAHash = serializer;
        config.deserializerEachEntityIsAValueInAHash = deserializer;
        return config;
    }

    private static <T> RedisRepositoryConfiguration<T> createBinaryEachEntityIsAValue(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final Function<T, byte[]> serializer,
            final Function<byte[], T> deserializer) {
        throwIfNull(serializer, "serializer");
        throwIfNull(deserializer, "deserializer");
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, EACH_ENTITY_IS_A_VALUE, true);
        config.binarySerializerEachEntityIsAValue = serializer;
        config.binaryDeserializerEachEntityIsAValue = deserializer;
        return config;
    }

    private static <T> RedisRepositoryConfiguration<T> createBinaryEachEntityIsAHash(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final Function<T, Map<byte[], byte[]>> serializer,
            final Function<Map<byte[], byte[]>, T> deserializer) {
        throwIfNull(serializer, "serializer");
        throwIfNull(deserializer, "deserializer");
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, EACH_ENTITY_IS_A_HASH, true);
        config.binarySerializerEachEntityIsAHash = serializer;
        config.binaryDeserializerEachEntityIsAHash = deserializer;
        return config;
    }

    private static <T> RedisRepositoryConfiguration<T> createBinaryEachEntityIsAValueInAHash(
            final JedisPool jedisPool,
            final Consumer<JedisException> jedisExceptionInterceptor,
            final String collectionKey,
            final String keySeparator,
            final Function<T, byte[]> serializer,
            final Function<byte[], T> deserializer) {
        throwIfNull(serializer, "serializer");
        throwIfNull(deserializer, "deserializer");
        final var config = new RedisRepositoryConfiguration<T>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, EACH_ENTITY_IS_A_VALUE_IN_A_HASH, true);
        config.binarySerializerEachEntityIsAValueInAHash = serializer;
        config.binaryDeserializerEachEntityIsAValueInAHash = deserializer;
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

    private static <K> void throwIfNull(final K object, final String valueName) {
        if (object == null) {
            throw new IllegalArgumentException(valueName + " cannot be null!");
        }
    }

    /**
     * Get a builder instance for the RedisRepositoryConfiguration object.
     * @return Builder object
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public final static class Builder<T> {

        private JedisPool jedisPool = null;
        private Consumer<JedisException> jedisExceptionInterceptor = null;
        private String collectionKey = null;
        private String keySeparator = ":";
        private boolean useBinaryApi = false;
        private RedisRepositoryStrategy strategy = NONE;
        private RedisRepositoryStrategy.EachEntityIsAValue<T> strategyEachEntityIsAValue;
        private RedisRepositoryStrategy.EachEntityIsAHash<T> strategyEachEntityIsAHash;
        private RedisRepositoryStrategy.EachEntityIsAValueInAHash<T> strategyEachEntityIsAValueInAHash;
        private RedisRepositoryStrategy.BinaryEachEntityIsAValue<T> strategyBinaryEachEntityIsAValue;
        private RedisRepositoryStrategy.BinaryEachEntityIsAHash<T> strategyBinaryEachEntityIsAHash;
        private RedisRepositoryStrategy.BinaryEachEntityIsAValueInAHash<T> strategyBinaryEachEntityIsAValueInAHash;

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

        public Builder<T> strategyEachEntityIsAValue(final EachEntityIsAValue<T> strategyEachEntityIsAValue) {
            this.useBinaryApi = false;
            this.strategy = EACH_ENTITY_IS_A_VALUE;
            this.strategyEachEntityIsAValue = strategyEachEntityIsAValue;
            return this;
        }

        public Builder<T> strategyEachEntityIsAHash(final EachEntityIsAHash<T> strategyEachEntityIsAHash) {
            this.useBinaryApi = false;
            this.strategy = EACH_ENTITY_IS_A_HASH;
            this.strategyEachEntityIsAHash = strategyEachEntityIsAHash;
            return this;
        }

        public Builder<T> strategyEachEntityIsAValueInAHash(final EachEntityIsAValueInAHash<T> strategyEachEntityIsAValueInAHash) {
            this.useBinaryApi = false;
            this.strategy = EACH_ENTITY_IS_A_VALUE_IN_A_HASH;
            this.strategyEachEntityIsAValueInAHash = strategyEachEntityIsAValueInAHash;
            return this;
        }

        public Builder<T> strategyBinaryEachEntityIsAValue(BinaryEachEntityIsAValue<T> strategyBinaryEachEntityIsAValue) {
            this.useBinaryApi = true;
            this.strategy = EACH_ENTITY_IS_A_VALUE;
            this.strategyBinaryEachEntityIsAValue = strategyBinaryEachEntityIsAValue;
            return this;
        }

        public Builder<T> strategyBinaryEachEntityIsAHash(BinaryEachEntityIsAHash<T> strategyBinaryEachEntityIsAHash) {
            this.useBinaryApi = true;
            this.strategy = EACH_ENTITY_IS_A_HASH;
            this.strategyBinaryEachEntityIsAHash = strategyBinaryEachEntityIsAHash;
            return this;
        }

        public Builder<T> strategyBinaryEachEntityIsAValueInAHash(BinaryEachEntityIsAValueInAHash<T> strategyBinaryEachEntityIsAValueInAHash) {
            this.useBinaryApi = true;
            this.strategy = EACH_ENTITY_IS_A_VALUE_IN_A_HASH;
            this.strategyBinaryEachEntityIsAValueInAHash = strategyBinaryEachEntityIsAValueInAHash;
            return this;
        }

        public RedisRepositoryConfiguration<T> build() {
            switch (strategy) {
                case EACH_ENTITY_IS_A_VALUE:
                    return useBinaryApi
                            ? createBinaryEachEntityIsAValue(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategyBinaryEachEntityIsAValue.getSerializer(), strategyBinaryEachEntityIsAValue.getDeserializer())
                            : createEachEntityIsAValue(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategyEachEntityIsAValue.getSerializer(), strategyEachEntityIsAValue.getDeserializer());
                case EACH_ENTITY_IS_A_HASH:
                    return useBinaryApi
                            ? createBinaryEachEntityIsAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategyBinaryEachEntityIsAHash.getSerializer(), strategyBinaryEachEntityIsAHash.getDeserializer())
                            : createEachEntityIsAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategyEachEntityIsAHash.getSerializer(), strategyEachEntityIsAHash.getDeserializer());
                case EACH_ENTITY_IS_A_VALUE_IN_A_HASH:
                    return useBinaryApi
                            ? createBinaryEachEntityIsAValueInAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategyBinaryEachEntityIsAValueInAHash.getSerializer(), strategyBinaryEachEntityIsAValueInAHash.getDeserializer())
                            : createEachEntityIsAValueInAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, strategyEachEntityIsAValueInAHash.getSerializer(), strategyEachEntityIsAValueInAHash.getDeserializer());
                default:
                    return new RedisRepositoryConfiguration<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, NONE, false);
            }

        }
    }
}
