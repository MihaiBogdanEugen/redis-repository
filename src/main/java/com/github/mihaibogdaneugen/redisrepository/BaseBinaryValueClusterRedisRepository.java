package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Optional;

public abstract class BaseBinaryValueClusterRedisRepository<T>
        extends ClusterRedisRepository
        implements BinaryValueClusterRedisRepository<T> {

    private final String keyPrefix;

    private byte[] sha1LuaScriptUpdateIfItIs;
    private byte[] sha1LuaScriptUpdateIfItIsNot;
    private byte[] sha1LuaScriptDeleteIfItIs;
    private byte[] sha1LuaScriptDeleteIfItIsNot;

    /**
     * Builds a BaseStringValueRedisRepository based on a configuration object.
     * @param configuration RedisRepositoryConfiguration object
     */
    public BaseBinaryValueClusterRedisRepository(final ClusterRedisRepositoryConfiguration configuration) {
        super(configuration.getJedisSlotBasedConnectionHandler(), configuration.getJedisExceptionInterceptor(), configuration.getMaxAttempts());
        this.keyPrefix = configuration.getCollectionKey() + configuration.getKeySeparator();
    }

    /**
     * Retrieves the entity with the given identifier.<br/>
     * Note: This method calls the GET Redis command.
     * @see <a href="https://redis.io/commands/GET">GET</a>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return runBinaryClusterCommand(key, jedis -> {
            final var entity = jedis.get(key);
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
     * Note: This method calls the SET Redis command.
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void set(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.set(key, convertTo(entity)));
    }

    /**
     * Sets the given entity with the specified identifier only if it does exist (update).<br/>
     * Note: This method calls the SET Redis command.
     * @see <a href="https://redis.io/commands/SET">SET</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfItExists(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.set(key, convertTo(entity), SetParams.setParams().xx()));
    }

    /**
     * Sets the given entity with the specified identifier only if it does not exist (insert).<br/>
     * Note: This method calls the SETNX Redis command.
     * @see <a href="https://redis.io/commands/SETNX">SETNX</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfDoesNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        runBinaryClusterCommand(key, jedis -> jedis.setnx(key, convertTo(entity)));
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
     * Sets the expiration after the given number of milliseconds for the entity with the given identifier.<br/>
     * Note: This method calls the PEXPIRE Redis command.
     * @see <a href="https://redis.io/commands/PEXPIRE">PEXPIRE</a>
     * @param id The String identifier of the entity
     * @param milliseconds The number of milliseconds after which the entity will expire
     */
    @Override
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
    @Override
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
    @Override
    public final Long getTimeToLiveLeft(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        final var key = getKey(id);
        return runBinaryClusterCommand(key, jedis -> jedis.pttl(key));
    }


    /**
     * Update the entity identified by the given identifier to the new provided value if its old value is equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. The script is cached on its first use.<br/>
     * If the entity is not there, this method does nothing.<br/>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     * @param newValue The new value of the entity
     */
    @Override
    public final void updateIfItIs(final String id, final T oldValue, final T newValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        throwIfNull(newValue, "newValue");
        final var key = getKey(id);
        if (isNullOrEmpty(sha1LuaScriptUpdateIfItIs)) {
            sha1LuaScriptUpdateIfItIs = runBinaryClusterCommand(key, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptUpdateIfItIs())));
        }
        final var keys = List.of(key);
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        runBinaryClusterCommand(key, jedis -> jedis.evalsha(sha1LuaScriptUpdateIfItIs, keys, args));
    }

    /**
     * Update the entity identified by the given identifier to the new provided value if its old value is not equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. The script is cached on its first use.<br/>
     * If the entity is not there, this method does nothing.<br/>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     * @param newValue The new value of the entity
     */
    @Override
    public final void updateIfItIsNot(final String id, final T oldValue, final T newValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        throwIfNull(newValue, "newValue");
        final var key = getKey(id);
        if (isNullOrEmpty(sha1LuaScriptUpdateIfItIsNot)) {
            sha1LuaScriptUpdateIfItIsNot = runBinaryClusterCommand(key, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptUpdateIfItIsNot())));
        }
        final var keys = List.of(key);
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        runBinaryClusterCommand(key, jedis -> jedis.evalsha(sha1LuaScriptUpdateIfItIsNot, keys, args));
    }

    /**
     * Delete the entity identified by the given identifier if its old value is equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. The script is cached on its first use.<br/>
     * If the entity is not there, this method does nothing.<br/>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     */
    @Override
    public final void deleteIfItIs(final String id, final T oldValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        final var key = getKey(id);
        if (isNullOrEmpty(sha1LuaScriptDeleteIfItIs)) {
            sha1LuaScriptDeleteIfItIs = runBinaryClusterCommand(key, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptDeleteIfItIs())));
        }
        final var keys = List.of(key);
        final var args = List.of(convertTo(oldValue));
        runBinaryClusterCommand(key, jedis -> jedis.evalsha(sha1LuaScriptDeleteIfItIs, keys, args));
    }

    /**
     * Delete the entity identified by the given identifier if its old value is not equal with the given one.<br/>
     * This method is using a Lua script to do this in a transactional manner. The script is cached on its first use.<br/>
     * If the entity is not there, this method does nothing.<br/>
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     */
    @Override
    public final void deleteIfItIsNot(final String id, final T oldValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(oldValue, "oldValue");
        final var key = getKey(id);
        if (isNullOrEmpty(sha1LuaScriptDeleteIfItIsNot)) {
            sha1LuaScriptDeleteIfItIsNot = runBinaryClusterCommand(key, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptDeleteIfItIsNot())));
        }
        final var keys = List.of(key);
        final var args = List.of(convertTo(oldValue));
        runBinaryClusterCommand(key, jedis -> jedis.evalsha(sha1LuaScriptDeleteIfItIsNot, keys, args));
    }

    private byte[] getKey(final String keySuffix) {
        return SafeEncoder.encode(keyPrefix + keySuffix);
    }
}
