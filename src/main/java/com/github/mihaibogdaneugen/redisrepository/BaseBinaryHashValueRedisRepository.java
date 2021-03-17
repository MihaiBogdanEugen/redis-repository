package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * A RedisRepository for a specified entity type, where all entities are serialized as binary values <br/>
 * and stored in a single map (Redis hash). Each entity is a key in this map. <br/>
 * Design details:<br/>
 * - every entity has a String identifier, but this is not enforced as part of the type itself.<br/>
 * - all entity of a certain type are stored into a single big hash, identified by the collection key.<br/>
 * Note: This repository optimises for `get_all` and `delete_all` operations, while not sacrificing performance<br/>
 * or transactional behaviour of other operations, with the except of `update`. There is no easy way to have a <br/>
 * transactional update, for such purposes, use a Lua script with the ScriptingCommands API. <br/>
 * @param <T> The type of the entity
 */
public abstract class BaseBinaryHashValueRedisRepository<T>
        extends RedisRepository
        implements BinaryHashValueRedisRepository<T> {

    private final byte[] parentKey;

    private byte[] sha1LuaScriptUpdateIfItIs;
    private byte[] sha1LuaScriptUpdateIfItIsNot;
    private byte[] sha1LuaScriptDeleteIfItIs;
    private byte[] sha1LuaScriptDeleteIfItIsNot;

    /**
     * Builds a BaseBinaryHashValueRedisRepository, based on a jedisPool object, for a specific collection.<br/>
     * For every operation, a Jedis object is retrieved from the pool and closed at the end.
     * @param jedisPool The JedisPool object
     * @param parentKey The name of the collection used as the parent key
     */
    public BaseBinaryHashValueRedisRepository(final JedisPool jedisPool, final String parentKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        if (parentKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Parent key `" + parentKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`!");
        }
        this.parentKey = SafeEncoder.encode(parentKey);
    }

    /**
     * Builds a BaseBinaryHashValueRedisRepository, based on a jedisPool object, for a specific collection, with an interceptor for JedisExceptions.<br/>
     * For every operation, a Jedis object is retrieved from the pool and closed at the end.
     * @param jedisPool The JedisPool object
     * @param parentKey The name of the collection used as the parent key
     * @param jedisExceptionInterceptor Consumer of errors of type JedisException
     */
    public BaseBinaryHashValueRedisRepository(final JedisPool jedisPool, final String parentKey, final Consumer<JedisException> jedisExceptionInterceptor) {
        super(jedisPool, jedisExceptionInterceptor);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        if (parentKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Parent key `" + parentKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`!");
        }
        this.parentKey = SafeEncoder.encode(parentKey);
    }

    /**
     * Retrieves the entity with the given identifier.<br/>
     * Note: This method calls the HGET Redis command.
     * @see <a href="https://redis.io/commands/HGET">HGET</a>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    @Override
    public final Optional<T> get(final String id) {
        throwIfNullOrEmptyOrBlank(id, "id");
        return getResult(jedis -> {
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
    public final Set<T> get(final Set<String> ids) {
        throwIfNullOrEmpty(ids, "ids");
        return getResult(jedis -> {
            final var entities = jedis.hmget(parentKey, getKeys(ids));
            return entities.stream()
                    .filter(RedisRepository::isNotNullNorEmpty)
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
    public final Set<T> getAll() {
        return getResult(jedis -> jedis.hgetAll(parentKey).values().stream()
                .filter(RedisRepository::isNotNullNorEmpty)
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
        return getResult(jedis -> jedis.hexists(parentKey, SafeEncoder.encode(id)));
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
        execute(jedis -> jedis.hset(parentKey, SafeEncoder.encode(id), convertTo(entity)));
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
        execute(jedis -> jedis.hsetnx(parentKey, SafeEncoder.encode(id), convertTo(entity)));
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
        execute(jedis -> jedis.hdel(parentKey, SafeEncoder.encode(id)));
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
        execute(jedis -> jedis.hdel(parentKey, getKeys(ids)));
    }

    /**
     * Removes all entities from the current collection.<br/>
     * Note: This method calls the DEL Redis command.
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     */
    @Override
    public final void deleteAll() {
        execute(jedis -> jedis.del(parentKey));
    }

    /**
     * Retrieve all keys of all entities in the current collection.<br/>
     * Note: This method calls the HKEYS Redis command.
     * @see <a href="https://redis.io/commands/HKEYS">HKEYS</a>
     * @return Set of String objects representing entity identifiers
     */
    @Override
    public Set<String> getAllKeys() {
        return getResult(jedis -> jedis.hkeys(SafeEncoder.encode(parentKey)));
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
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        execute(jedis -> {
            if (isNullOrEmpty(sha1LuaScriptUpdateIfItIs)) {
                sha1LuaScriptUpdateIfItIs = jedis.scriptLoad(SafeEncoder.encode(getLuaScriptUpdateIfItIs()));
            }
            jedis.evalsha(sha1LuaScriptUpdateIfItIs, keys, args);
        });
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
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        execute(jedis -> {
            if (isNullOrEmpty(sha1LuaScriptUpdateIfItIsNot)) {
                sha1LuaScriptUpdateIfItIsNot = jedis.scriptLoad(SafeEncoder.encode(getLuaScriptUpdateIfItIsNot()));
            }
            jedis.evalsha(sha1LuaScriptUpdateIfItIsNot, keys, args);
        });
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
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue));
        execute(jedis -> {
            if (isNullOrEmpty(sha1LuaScriptDeleteIfItIs)) {
                sha1LuaScriptDeleteIfItIs = jedis.scriptLoad(SafeEncoder.encode(getLuaScriptDeleteIfItIs()));
            }
            jedis.evalsha(sha1LuaScriptDeleteIfItIs, keys, args);
        });
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
        final var keys = List.of(parentKey, SafeEncoder.encode(id));
        final var args = List.of(convertTo(oldValue));
        execute(jedis -> {
            if (isNullOrEmpty(sha1LuaScriptDeleteIfItIsNot)) {
                sha1LuaScriptDeleteIfItIsNot = jedis.scriptLoad(SafeEncoder.encode(getLuaScriptDeleteIfItIsNot()));
            }
            jedis.evalsha(sha1LuaScriptDeleteIfItIsNot, keys, args);
        });
    }

    private byte[][] getKeys(final Set<String> ids) {
        return ids.stream()
                .filter(RedisRepository::isNotNullNorEmptyNorBlank)
                .map(SafeEncoder::encode)
                .toArray(byte[][]::new);
    }
}
