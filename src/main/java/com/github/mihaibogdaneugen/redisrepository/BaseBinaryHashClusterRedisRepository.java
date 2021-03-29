package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.util.SafeEncoder;

import java.util.Map;
import java.util.Optional;

public abstract class BaseBinaryHashClusterRedisRepository<T>
        extends ClusterRedisRepository
        implements BinaryHashClusterRedisRepository<T> {

    private final String keyPrefix;

    /**
     * Builds a BaseStringHashRedisRepository based on a configuration object.
     * @param configuration RedisRepositoryConfiguration object
     */
    public BaseBinaryHashClusterRedisRepository(final ClusterRedisRepositoryConfiguration configuration) {
        super(configuration.getJedisSlotBasedConnectionHandler(), configuration.getJedisExceptionInterceptor(), configuration.getMaxAttempts());
        this.keyPrefix = configuration.getCollectionKey() + configuration.getKeySeparator();
    }

    /**
     * Retrieves the entity with the given identifier.<br/>
     * Note: This method calls the HGETALL Redis command.
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return runBinaryClusterCommand(key, jedis -> {
            final var entity = jedis.hgetAll(key);
            return isNullOrEmpty(entity)
                    ? Optional.empty()
                    : Optional.of(convertFrom(entity));
        });
    }

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.<br/>
     * Note: This method calls the EXISTS Redis command.
     * @see <a href="https://redis.io/commands/EXISTS">EXISTS</a>
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return runBinaryClusterCommand(key, jedis -> jedis.exists(key));
    }

    /**
     * Sets (updates or inserts) the given entity with the specified identifier.<br/>
     * Note: This method calls the HSET Redis command.
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.hset(key, convertTo(entity)));
    }

    /**
     * Removes the entity with the given identifier.<br/>
     * Note: This method calls the DEL Redis command.
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @param id The String identifier of the entity
     */
    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.del(key));
    }

    /**
     * Replaces (or inserts) a specific field of an entity identified by the given identifier.<br/>
     * Note: This method calls the HSET Redis command.
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @param id The String identifier of the entity
     * @param fieldAndValue A Map.Entry pair of a field and value, serialized as String objects
     */
    public final void setField(final String id, final Map.Entry<byte[], byte[]> fieldAndValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(fieldAndValue, "fieldAndValue");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.hset(key, fieldAndValue.getKey(), fieldAndValue.getValue()));
    }

    /**
     * Inserts a specific field of an entity identified by the given identifier, only if it does not exist.<br/>
     * Note: This method calls the HSETNX Redis command.
     * @see <a href="https://redis.io/commands/HSETNX">HSETNX</a>
     * @param id The String identifier of the entity
     * @param fieldAndValue A Map.Entry pair of a field and value, serialized as String objects
     */
    public final void setFieldIfNotExists(final String id, final Map.Entry<byte[], byte[]> fieldAndValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(fieldAndValue, "fieldAndValue");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.hsetnx(key, fieldAndValue.getKey(), fieldAndValue.getValue()));
    }

    /**
     * Sets the expiration after the given number of milliseconds for the entity with the given identifier.<br/>
     * Note: This method calls the PEXPIRE Redis command.
     * @see <a href="https://redis.io/commands/PEXPIRE">PEXPIRE</a>
     * @param id The String identifier of the entity
     * @param milliseconds The number of milliseconds after which the entity will expire
     */
    public final void setExpirationAfter(final String id, final long milliseconds) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNegative(milliseconds, "milliseconds");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.pexpire(key, milliseconds));
    }

    /**
     * Sets the expiration at the given timestamp (Unix time) for the entity with the given identifier.<br/>
     * Note: This method calls the PEXPIREAT Redis command.
     * @see <a href="https://redis.io/commands/PEXPIREAT">PEXPIREAT</a>
     * @param id The String identifier of the entity
     * @param millisecondsTimestamp The timestamp (Unix time) when the entity will expire
     */
    public final void setExpirationAt(final String id, final long millisecondsTimestamp) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNegative(millisecondsTimestamp, "millisecondsTimestamp");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.pexpireAt(key, millisecondsTimestamp));
    }

    /**
     * Returns the time to live left in milliseconds till the entity will expire.<br/>
     * Note: This method calls the PTTL Redis command.
     * @see <a href="https://redis.io/commands/PTTL">PTTL</a>
     * @param id The String identifier of the entity
     * @return No. of milliseconds
     */
    public final Long getTimeToLiveLeft(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return runBinaryClusterCommand(key, jedis -> jedis.pttl(key));
    }

    private byte[] getKey(final String keySuffix) {
        return SafeEncoder.encode(keyPrefix + keySuffix);
    }
}