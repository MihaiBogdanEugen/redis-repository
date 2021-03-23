package com.github.mihaibogdaneugen.redisrepository.cluster;

import com.github.mihaibogdaneugen.redisrepository.BinaryHashValueRedisRepository;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BaseBinaryHashValueClusterRedisRepository<T>
    extends ClusterRedisRepository
    implements BinaryHashValueRedisRepository<T> {

    private final byte[] parentKey;

    private byte[] sha1LuaScriptUpdateIfItIs;
    private byte[] sha1LuaScriptUpdateIfItIsNot;
    private byte[] sha1LuaScriptDeleteIfItIs;
    private byte[] sha1LuaScriptDeleteIfItIsNot;

    public BaseBinaryHashValueClusterRedisRepository(final ClusterRedisRepositoryConfiguration configuration) {
        super(configuration.getJedisSlotBasedConnectionHandler(), configuration.getJedisExceptionInterceptor(), configuration.getMaxAttempts());
        this.parentKey = SafeEncoder.encode(configuration.getCollectionKey());
    }

    /**
     * Retrieves the entity with the given identifier.<br/>
     * Note: This method calls the HGET Redis command.
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    @Override
    public Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        return runBinaryClusterCommand(parentKey, jedis -> {
            final var entity = jedis.hget(parentKey, SafeEncoder.encode(id));
            return isNullOrEmpty(entity)
                    ? Optional.empty()
                    : Optional.of(convertFrom(entity));
        });
    }

    /**
     * Retrieves the entities with the given identifiers.<br/>
     * Note: This method calls the HMGET Redis command.
     * @see <a href="https://redis.io/commands/HMGET">HMGET</a>
     * @param ids The set of Strings identifiers of entities
     * @return A set of entities
     */
    @Override
    public Set<T> get(final Set<String> ids) {
        throwIfNullOrEmpty(ids, "ids");
        return runBinaryClusterCommand(parentKey, jedis -> {
            final var entities = jedis.hmget(parentKey, getKeys(ids));
            return entities.stream()
                    .filter(ClusterRedisRepository::isNotNullNorEmpty)
                    .map(this::convertFrom)
                    .collect(Collectors.toSet());
        });
    }

    /**
     * Retrieves all entities from the current collection.<br/>
     * Note: This method calls the HGETALL Redis command.
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @return A set of entities
     */
    @Override
    public Set<T> getAll() {
        return runBinaryClusterCommand(parentKey, jedis -> jedis.hgetAll(parentKey).values().stream()
                .filter(ClusterRedisRepository::isNotNullNorEmpty)
                .map(this::convertFrom)
                .collect(Collectors.toSet()));
    }

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.<br/>
     * Note: This method calls the HEXISTS Redis command.
     * @see <a href="https://redis.io/commands/HEXISTS">HEXISTS</a>
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    @Override
    public final Boolean exists(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        return runBinaryClusterCommand(parentKey, jedis -> jedis.hexists(parentKey, SafeEncoder.encode(id)));
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
        runBinaryClusterCommand(parentKey, jedis -> jedis.hset(parentKey, SafeEncoder.encode(id), convertTo(entity)));
    }

    /**
     * Sets the given entity with the specified identifier only if it does not exist (insert).<br/>
     * Note: This method calls the HSETNX Redis command.
     * @see <a href="https://redis.io/commands/HSETNX">HSETNX</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfDoesNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        runBinaryClusterCommand(parentKey, jedis -> jedis.hsetnx(parentKey, SafeEncoder.encode(id), convertTo(entity)));
    }

    /**
     * Removes the entity with the given identifier.<br/>
     * Note: This method calls the HDEL Redis command.
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @param id The String identifier of the entity
     */
    @Override
    public final void delete(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        runBinaryClusterCommand(parentKey, jedis -> jedis.hdel(parentKey, SafeEncoder.encode(id)));
    }

    /**
     * Removes all entities with the given identifiers.<br/>
     * Note: This method calls the HDEL Redis command.
     * @see <a href="https://redis.io/commands/HDEL">HDEL</a>
     * @param ids The set of Strings identifiers of entities
     */
    @Override
    public final void delete(final Set<String> ids) {
        throwIfNullOrEmpty(ids, "ids");
        runBinaryClusterCommand(parentKey, jedis -> jedis.hdel(parentKey, getKeys(ids)));
    }

    /**
     * Removes all entities from the current collection.<br/>
     * Note: This method calls the DEL Redis command.
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     */
    @Override
    public final void deleteAll() {
        runBinaryClusterCommand(parentKey, jedis -> jedis.del(parentKey));
    }

    /**
     * Retrieve all keys of all entities in the current collection.<br/>
     * Note: This method calls the HKEYS Redis command.
     * @see <a href="https://redis.io/commands/HKEYS">HKEYS</a>
     * @return Set of String objects representing entity identifiers
     */
    @Override
    public Set<String> getAllKeys() {
        return runBinaryClusterCommand(parentKey, jedis -> jedis.hkeys(SafeEncoder.encode(parentKey)));
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
        if (isNullOrEmpty(sha1LuaScriptUpdateIfItIs)) {
            sha1LuaScriptUpdateIfItIs = runBinaryClusterCommand(parentKey, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptUpdateIfItIs())));
        }
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        runBinaryClusterCommand(new HashSet<>(keys), jedis -> jedis.evalsha(sha1LuaScriptUpdateIfItIs, keys, args));
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
        if (isNullOrEmpty(sha1LuaScriptUpdateIfItIsNot)) {
            sha1LuaScriptUpdateIfItIsNot = runBinaryClusterCommand(parentKey, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptUpdateIfItIsNot())));
        }
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        runBinaryClusterCommand(new HashSet<>(keys), jedis -> jedis.evalsha(sha1LuaScriptUpdateIfItIsNot, keys, args));
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
        if (isNullOrEmpty(sha1LuaScriptDeleteIfItIs)) {
            sha1LuaScriptDeleteIfItIs = runBinaryClusterCommand(parentKey, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptDeleteIfItIs())));
        }
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue));
        runBinaryClusterCommand(new HashSet<>(keys), jedis -> jedis.evalsha(sha1LuaScriptDeleteIfItIs, keys, args));
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
        if (isNullOrEmpty(sha1LuaScriptDeleteIfItIsNot)) {
            sha1LuaScriptDeleteIfItIsNot = runBinaryClusterCommand(parentKey, jedis -> jedis.scriptLoad(SafeEncoder.encode(getLuaScriptDeleteIfItIsNot())));
        }
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue));
        runBinaryClusterCommand(new HashSet<>(keys), jedis -> jedis.evalsha(sha1LuaScriptDeleteIfItIsNot, keys, args));
    }

    private byte[][] getKeys(final Set<String> ids) {
        return ids.stream()
                .filter(ClusterRedisRepository::isNotNullNorEmptyNorBlank)
                .map(SafeEncoder::encode)
                .toArray(byte[][]::new);
    }
}
