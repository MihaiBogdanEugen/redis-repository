package com.github.mihaibogdaneugen.redisrepository;

public class RedisRepositoryConfiguration {

    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final Character DEFAULT_KEY_SEPARATOR = ':';

    private final String keyPrefix;
    private final int batchSize;

    private RedisRepositoryConfiguration(final String collectionKey, final String keySeparator, final int batchSize) {

        if (isNullOrEmptyOrBlank(collectionKey)) {
            throw new IllegalArgumentException("If key separator is specified the collection key cannot be null nor empty");
        }

        if (isNullOrEmptyOrBlank(keySeparator)) {
            keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        } else {
            keyPrefix = collectionKey + keySeparator;
        }

        this.batchSize = batchSize < 1 ? DEFAULT_BATCH_SIZE : batchSize;
    }

    private static boolean isNullOrEmptyOrBlank(final String text) {
        return text == null || text.isEmpty() || text.isBlank();
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public final static class Builder {

        private String collectionKey = "";
        private String keySeparator = "";
        private int batchSize = 0;

        public Builder collectionKey(final String collectionKey) {
            this.collectionKey = collectionKey;
            return this;
        }

        public Builder keySeparator(final String keySeparator) {
            this.keySeparator = keySeparator;
            return this;
        }

        public Builder batchSize(final int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public RedisRepositoryConfiguration build() {
            return new RedisRepositoryConfiguration(collectionKey, keySeparator, batchSize);
        }
    }
}
