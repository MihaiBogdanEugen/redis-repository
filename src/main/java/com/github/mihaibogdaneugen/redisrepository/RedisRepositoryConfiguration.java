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
            throw new IllegalArgumentException("Invalid RedisRepositoryStrategy");
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

    public static class Builder<T> {

        private JedisPool jedisPool = null;
        private Consumer<JedisException> jedisExceptionInterceptor = null;
        private String collectionKey = null;
        private String keySeparator = ":";
        private boolean binaryApi = false;

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

        public RedisRepositoryConfiguration<T> build() {
            return new RedisRepositoryConfiguration<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, NONE, false);
        }

        public EachEntityIsAValueBuilder<T> strategyEachEntityIsAValue() {
            return new EachEntityIsAValueBuilder<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }

        public EachEntityIsAHashBuilder<T> strategyEachEntityIsAHash() {
            return new EachEntityIsAHashBuilder<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }

        public EachEntityIsAValueInAHashBuilder<T> strategyEachEntityIsAValueInAHash() {
            return new EachEntityIsAValueInAHashBuilder<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }

        public BinaryEachEntityIsAValueBuilder<T> strategyEachEntityIsAValueBinary() {
            return new BinaryEachEntityIsAValueBuilder<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }

        public BinaryEachEntityIsAHashBinaryBuilder<T> strategyEachEntityIsAHashBinary() {
            return new BinaryEachEntityIsAHashBinaryBuilder<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }

        public BinaryEachEntityIsAValueInAHashBinaryBuilder<T> strategyEachEntityIsAValueInAHashBinary() {
            return new BinaryEachEntityIsAValueInAHashBinaryBuilder<>(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator);
        }

        public static class EachEntityIsAValueBuilder<T> {

            private final JedisPool jedisPool;
            private final Consumer<JedisException> jedisExceptionInterceptor;
            private final String collectionKey;
            private final String keySeparator;
            private Function<T, String> serializer = null;
            private Function<String, T> deserializer = null;

            private EachEntityIsAValueBuilder(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {
                this.jedisPool = jedisPool;
                this.jedisExceptionInterceptor = jedisExceptionInterceptor;
                this.collectionKey = collectionKey;
                this.keySeparator = keySeparator;
            }

            public EachEntityIsAValueBuilder<T> serializer(final Function<T, String> serializer) {
                this.serializer = serializer;
                return this;
            }

            public EachEntityIsAValueBuilder<T> deserializer(final Function<String, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public RedisRepositoryConfiguration<T> build() {
                return createEachEntityIsAValue(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, serializer, deserializer);
            }
        }

        public static class EachEntityIsAHashBuilder<T> {

            private final JedisPool jedisPool;
            private final Consumer<JedisException> jedisExceptionInterceptor;
            private final String collectionKey;
            private final String keySeparator;
            private Function<T, Map<String, String>> serializer = null;
            private Function<Map<String, String>, T> deserializer = null;

            private EachEntityIsAHashBuilder(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {
                this.jedisPool = jedisPool;
                this.jedisExceptionInterceptor = jedisExceptionInterceptor;
                this.collectionKey = collectionKey;
                this.keySeparator = keySeparator;
            }

            public EachEntityIsAHashBuilder<T> serializer(Function<T, Map<String, String>> serializer) {
                this.serializer = serializer;
                return this;
            }

            public EachEntityIsAHashBuilder<T> deserializer(Function<Map<String, String>, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public RedisRepositoryConfiguration<T> build() {
                return createEachEntityIsAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, serializer, deserializer);
            }
        }

        public static class EachEntityIsAValueInAHashBuilder<T> {

            private final JedisPool jedisPool;
            private final Consumer<JedisException> jedisExceptionInterceptor;
            private final String collectionKey;
            private final String keySeparator;
            private Function<T, String> serializer = null;
            private Function<String, T> deserializer = null;

            private EachEntityIsAValueInAHashBuilder(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {
                this.jedisPool = jedisPool;
                this.jedisExceptionInterceptor = jedisExceptionInterceptor;
                this.collectionKey = collectionKey;
                this.keySeparator = keySeparator;
            }

            public EachEntityIsAValueInAHashBuilder<T> serializer(Function<T, String> serializer) {
                this.serializer = serializer;
                return this;
            }

            public EachEntityIsAValueInAHashBuilder<T> deserializer(Function<String, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public RedisRepositoryConfiguration<T> build() {
                return createEachEntityIsAValueInAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, serializer, deserializer);
            }
        }

        public static class BinaryEachEntityIsAValueBuilder<T> {

            private final JedisPool jedisPool;
            private final Consumer<JedisException> jedisExceptionInterceptor;
            private final String collectionKey;
            private final String keySeparator;
            private Function<T, byte[]> serializer = null;
            private Function<byte[], T> deserializer = null;

            private BinaryEachEntityIsAValueBuilder(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {
                this.jedisPool = jedisPool;
                this.jedisExceptionInterceptor = jedisExceptionInterceptor;
                this.collectionKey = collectionKey;
                this.keySeparator = keySeparator;
            }

            public BinaryEachEntityIsAValueBuilder<T> serializer(final Function<T, byte[]> serializer) {
                this.serializer = serializer;
                return this;
            }

            public BinaryEachEntityIsAValueBuilder<T> deserializer(final Function<byte[], T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public RedisRepositoryConfiguration<T> build() {
                return createBinaryEachEntityIsAValue(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, serializer, deserializer);
            }
        }

        public static class BinaryEachEntityIsAHashBinaryBuilder<T> {

            private final JedisPool jedisPool;
            private final Consumer<JedisException> jedisExceptionInterceptor;
            private final String collectionKey;
            private final String keySeparator;
            private Function<T, Map<byte[], byte[]>> serializer = null;
            private Function<Map<byte[], byte[]>, T> deserializer = null;

            private BinaryEachEntityIsAHashBinaryBuilder(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {
                this.jedisPool = jedisPool;
                this.jedisExceptionInterceptor = jedisExceptionInterceptor;
                this.collectionKey = collectionKey;
                this.keySeparator = keySeparator;
            }

            public BinaryEachEntityIsAHashBinaryBuilder<T> serializer(Function<T, Map<byte[], byte[]>> serializer) {
                this.serializer = serializer;
                return this;
            }

            public BinaryEachEntityIsAHashBinaryBuilder<T> deserializer(Function<Map<byte[], byte[]>, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public RedisRepositoryConfiguration<T> build() {
                return createBinaryEachEntityIsAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, serializer, deserializer);
            }
        }

        public static class BinaryEachEntityIsAValueInAHashBinaryBuilder<T> {

            private final JedisPool jedisPool;
            private final Consumer<JedisException> jedisExceptionInterceptor;
            private final String collectionKey;
            private final String keySeparator;
            private Function<T, byte[]> serializer = null;
            private Function<byte[], T> deserializer = null;

            private BinaryEachEntityIsAValueInAHashBinaryBuilder(final JedisPool jedisPool, final Consumer<JedisException> jedisExceptionInterceptor, final String collectionKey, final String keySeparator) {
                this.jedisPool = jedisPool;
                this.jedisExceptionInterceptor = jedisExceptionInterceptor;
                this.collectionKey = collectionKey;
                this.keySeparator = keySeparator;
            }

            public BinaryEachEntityIsAValueInAHashBinaryBuilder<T> serializer(Function<T, byte[]> serializer) {
                this.serializer = serializer;
                return this;
            }

            public BinaryEachEntityIsAValueInAHashBinaryBuilder<T> deserializer(Function<byte[], T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public RedisRepositoryConfiguration<T> build() {
                return createBinaryEachEntityIsAValueInAHash(jedisPool, jedisExceptionInterceptor, collectionKey, keySeparator, serializer, deserializer);
            }
        }
    }
}
