package com.github.mihaibogdaneugen.redisrepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A RedisRepository for a specified entity type, where all entities are serialized as UTF-8 encoded Strings <br/>
 * and stored in a single map (Redis hash). Each entity is a key in this map. <br/>
 * Design details:<br/>
 * - every entity has a String identifier, but this is not enforced as part of the type itself.<br/>
 * - all entity of a certain type are stored into a single big hash, identified by the collection key.<br/>
 * Note: This repository optimises for `get_all` and `delete_all` operations, while not sacrificing performance<br/>
 * or transactional behaviour of other operations, with the except of `update`. There is no easy way to have a <br/>
 * transactional update, for such purposes, use a Lua script with the ScriptingCommands API. <br/>
 * @param <T> The type of the entity
 */
public abstract class BaseStringHashValueRedisRepository<T>
        extends RedisRepository
        implements StringHashValueRedisRepository<T> {

    private final String parentKey;

    private String sha1LuaScriptUpdateIfItIs;
    private String sha1LuaScriptUpdateIfItIsNot;
    private String sha1LuaScriptDeleteIfItIs;
    private String sha1LuaScriptDeleteIfItIsNot;

    /**
     * Builds a BaseStringHashValueRedisRepository based on a configuration object.
     * @param configuration RedisRepositoryConfiguration object
     */
    public BaseStringHashValueRedisRepository(final RedisRepositoryConfiguration configuration) {
        super(configuration.getJedisPool(), configuration.getJedisExceptionInterceptor());
        this.parentKey = configuration.getCollectionKey();
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
            final var entity = jedis.hget(parentKey, id);
            return isNullOrEmptyOrBlank(entity)
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
            final var entities = jedis.hmget(parentKey, ids.toArray(String[]::new));
            return entities.stream()
                    .filter(RedisRepository::isNotNullNorEmptyNorBlank)
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
                .filter(RedisRepository::isNotNullNorEmptyNorBlank)
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
        return getResult(jedis -> jedis.hexists(parentKey, id));
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
        execute(jedis -> jedis.hset(parentKey, id, convertTo(entity)));
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
        execute(jedis -> jedis.hsetnx(parentKey, id, convertTo(entity)));
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
        execute(jedis -> jedis.hdel(parentKey, id));
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
        execute(jedis -> jedis.hdel(parentKey, ids.toArray(String[]::new)));
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
        return getResult(jedis -> jedis.hkeys(parentKey));
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
        final var keys = List.of(parentKey, id);
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        execute(jedis -> {
            if (isNullOrEmptyOrBlank(sha1LuaScriptUpdateIfItIs)) {
                sha1LuaScriptUpdateIfItIs = jedis.scriptLoad(getLuaScriptUpdateIfItIs());
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
        final var keys = List.of(parentKey, id);
        final var args = List.of(convertTo(oldValue), convertTo(newValue));
        execute(jedis -> {
            if (isNullOrEmptyOrBlank(sha1LuaScriptUpdateIfItIsNot)) {
                sha1LuaScriptUpdateIfItIsNot = jedis.scriptLoad(getLuaScriptUpdateIfItIsNot());
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
        final var keys = List.of(parentKey, id);
        final var args = List.of(convertTo(oldValue));
        execute(jedis -> {
            if (isNullOrEmptyOrBlank(sha1LuaScriptDeleteIfItIs)) {
                sha1LuaScriptDeleteIfItIs = jedis.scriptLoad(getLuaScriptDeleteIfItIs());
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
        final var keys = List.of(parentKey, id);
        final var args = List.of(convertTo(oldValue));
        execute(jedis -> {
            if (isNullOrEmptyOrBlank(sha1LuaScriptDeleteIfItIsNot)) {
                sha1LuaScriptDeleteIfItIsNot = jedis.scriptLoad(getLuaScriptDeleteIfItIsNot());
            }
            jedis.evalsha(sha1LuaScriptDeleteIfItIsNot, keys, args);
        });
    }
}
