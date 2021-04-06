package com.github.mihaibogdaneugen.redisrepository;

import java.util.Map;
import java.util.function.Function;

public enum RedisRepositoryStrategy {

    NONE,
    EACH_ENTITY_IS_A_VALUE,
    EACH_ENTITY_IS_A_HASH,
    EACH_ENTITY_IS_A_VALUE_IN_A_HASH;

    public static final class EachEntityIsAValue<T> {

        private final Function<T, String> serializer;
        private final Function<String, T> deserializer;

        private EachEntityIsAValue(final Function<T, String> serializer, final Function<String, T> deserializer) {
            throwIfNull(serializer, "serializer");
            throwIfNull(deserializer, "deserializer");
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public Function<T, String> getSerializer() {
            return serializer;
        }

        public Function<String, T> getDeserializer() {
            return deserializer;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        private static <K> void throwIfNull(final K object, final String valueName) {
            if (object == null) {
                throw new IllegalArgumentException(valueName + " cannot be null!");
            }
        }

        public static final class Builder<T> {

            private Function<T, String> serializer = null;
            private Function<String, T> deserializer = null;

            public Builder<T> serializer(final Function<T, String> serializer) {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> deserializer(final Function<String, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public EachEntityIsAValue<T> build() {
                return new EachEntityIsAValue<>(serializer, deserializer);
            }
        }
    }

    public static final class EachEntityIsAHash<T> {

        private final Function<T, Map<String, String>> serializer;
        private final Function<Map<String, String>, T> deserializer;

        private EachEntityIsAHash(final Function<T, Map<String, String>> serializer, final Function<Map<String, String>, T> deserializer) {
            throwIfNull(serializer, "serializer");
            throwIfNull(deserializer, "deserializer");
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public Function<T, Map<String, String>> getSerializer() {
            return serializer;
        }

        public Function<Map<String, String>, T> getDeserializer() {
            return deserializer;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        private static <K> void throwIfNull(final K object, final String valueName) {
            if (object == null) {
                throw new IllegalArgumentException(valueName + " cannot be null!");
            }
        }

        public static final class Builder<T> {

            private Function<T, Map<String, String>> serializer = null;
            private Function<Map<String, String>, T> deserializer = null;

            public Builder<T> serializer(final Function<T, Map<String, String>> serializer) {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> deserializer(final Function<Map<String, String>, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public EachEntityIsAHash<T> build() {
                return new EachEntityIsAHash<>(serializer, deserializer);
            }
        }
    }

    public static final class EachEntityIsAValueInAHash<T> {

        private final Function<T, String> serializer;
        private final Function<String, T> deserializer;

        private EachEntityIsAValueInAHash(final Function<T, String> serializer, final Function<String, T> deserializer) {
            throwIfNull(serializer, "serializer");
            throwIfNull(deserializer, "deserializer");
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public Function<T, String> getSerializer() {
            return serializer;
        }

        public Function<String, T> getDeserializer() {
            return deserializer;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        private static <K> void throwIfNull(final K object, final String valueName) {
            if (object == null) {
                throw new IllegalArgumentException(valueName + " cannot be null!");
            }
        }

        public static final class Builder<T> {

            private Function<T, String> serializer = null;
            private Function<String, T> deserializer = null;

            public Builder<T> serializer(final Function<T, String> serializer) {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> deserializer(final Function<String, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public EachEntityIsAValueInAHash<T> build() {
                return new EachEntityIsAValueInAHash<>(serializer, deserializer);
            }
        }
    }

    public static final class BinaryEachEntityIsAValue<T> {

        private final Function<T, byte[]> serializer;
        private final Function<byte[], T> deserializer;

        private BinaryEachEntityIsAValue(final Function<T, byte[]> serializer, final Function<byte[], T> deserializer) {
            throwIfNull(serializer, "serializer");
            throwIfNull(deserializer, "deserializer");
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public Function<T, byte[]> getSerializer() {
            return serializer;
        }

        public Function<byte[], T> getDeserializer() {
            return deserializer;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        private static <K> void throwIfNull(final K object, final String valueName) {
            if (object == null) {
                throw new IllegalArgumentException(valueName + " cannot be null!");
            }
        }

        public static final class Builder<T> {

            private Function<T, byte[]> serializer = null;
            private Function<byte[], T> deserializer = null;

            public Builder<T> serializer(final Function<T, byte[]> serializer) {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> deserializer(final Function<byte[], T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public BinaryEachEntityIsAValue<T> build() {
                return new BinaryEachEntityIsAValue<>(serializer, deserializer);
            }
        }
    }

    public static final class BinaryEachEntityIsAHash<T> {

        private final Function<T, Map<byte[], byte[]>> serializer;
        private final Function<Map<byte[], byte[]>, T> deserializer;

        private BinaryEachEntityIsAHash(final Function<T, Map<byte[], byte[]>> serializer, final Function<Map<byte[], byte[]>, T> deserializer) {
            throwIfNull(serializer, "serializer");
            throwIfNull(deserializer, "deserializer");
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public Function<T, Map<byte[], byte[]>> getSerializer() {
            return serializer;
        }

        public Function<Map<byte[], byte[]>, T> getDeserializer() {
            return deserializer;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        private static <K> void throwIfNull(final K object, final String valueName) {
            if (object == null) {
                throw new IllegalArgumentException(valueName + " cannot be null!");
            }
        }

        public static final class Builder<T> {

            private Function<T, Map<byte[], byte[]>> serializer = null;
            private Function<Map<byte[], byte[]>, T> deserializer = null;

            public Builder<T> serializer(final Function<T, Map<byte[], byte[]>> serializer) {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> deserializer(final Function<Map<byte[], byte[]>, T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public BinaryEachEntityIsAHash<T> build() {
                return new BinaryEachEntityIsAHash<>(serializer, deserializer);
            }
        }
    }

    public static final class BinaryEachEntityIsAValueInAHash<T> {

        private final Function<T, byte[]> serializer;
        private final Function<byte[], T> deserializer;

        private BinaryEachEntityIsAValueInAHash(final Function<T, byte[]> serializer, final Function<byte[], T> deserializer) {
            throwIfNull(serializer, "serializer");
            throwIfNull(deserializer, "deserializer");
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        public Function<T, byte[]> getSerializer() {
            return serializer;
        }

        public Function<byte[], T> getDeserializer() {
            return deserializer;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        private static <K> void throwIfNull(final K object, final String valueName) {
            if (object == null) {
                throw new IllegalArgumentException(valueName + " cannot be null!");
            }
        }

        public static final class Builder<T> {

            private Function<T, byte[]> serializer = null;
            private Function<byte[], T> deserializer = null;

            public Builder<T> serializer(final Function<T, byte[]> serializer) {
                this.serializer = serializer;
                return this;
            }

            public Builder<T> deserializer(final Function<byte[], T> deserializer) {
                this.deserializer = deserializer;
                return this;
            }

            public BinaryEachEntityIsAValueInAHash<T> build() {
                return new BinaryEachEntityIsAValueInAHash<>(serializer, deserializer);
            }
        }
    }
}
