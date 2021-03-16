package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
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

    /**
     * Builds a BaseStringHashValueRedisRepository, based on a jedisPool object, for a specific collection.<br/>
     * For every operation, a Jedis object is retrieved from the pool and closed at the end.
     * @param jedisPool The JedisPool object
     * @param parentKey The name of the collection used as the parent key
     */
    public BaseStringHashValueRedisRepository(final JedisPool jedisPool, final String parentKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        if (parentKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Parent key `" + parentKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`!");
        }
        this.parentKey = parentKey;
    }

    /**
     * Builds a BaseStringHashValueRedisRepository, based on a jedisPool object, for a specific collection, with an interceptor for JedisExceptions.<br/>
     * For every operation, a Jedis object is retrieved from the pool and closed at the end.
     * @param jedisPool The JedisPool object
     * @param parentKey The name of the collection used as the parent key
     * @param jedisExceptionInterceptor Consumer of errors of type JedisException
     */
    public BaseStringHashValueRedisRepository(final JedisPool jedisPool, final String parentKey, final Consumer<JedisException> jedisExceptionInterceptor) {
        super(jedisPool, jedisExceptionInterceptor);
        throwIfNullOrEmptyOrBlank(parentKey, "parentKey");
        if (parentKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Parent key `" + parentKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`!");
        }
        this.parentKey = parentKey;
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
     * @param ids The array of Strings identifiers of entities
     * @return A list of entities
     */
    @Override
    public final List<T> get(final String... ids) {
        throwIfNullOrEmpty(ids, "ids");
        return getResult(jedis -> {
            final var entities = jedis.hmget(parentKey, ids);
            return entities.stream()
                    .filter(RedisRepository::isNotNullNorEmptyNorBlank)
                    .map(this::convertFrom)
                    .collect(Collectors.toList());
        });
    }

    /**
     * Retrieves all entities from the current collection.<br/>
     * Note: This method calls the HGETALL Redis command.
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @return A list of entities
     */
    @Override
    public final List<T> getAll() {
        return getResult(jedis -> jedis.hgetAll(parentKey).values().stream()
                .filter(RedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::convertFrom)
                .collect(Collectors.toList()));
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
     * Replaces (or inserts) the given entity with the specified identifier.<br/>
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
     * Inserts the given entity with the specified identifier, only if it does not exist.<br/>
     * Note: This method calls the HSETNX Redis command.
     * @see <a href="https://redis.io/commands/HSETNX">HSETNX</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfNotExist(final String id, final T entity) {
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
     * @param ids The array of Strings identifiers of entities
     */
    @Override
    public final void delete(final String... ids) {
        throwIfNullOrEmpty(ids, "ids");
        execute(jedis -> jedis.hdel(parentKey, ids));
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
}
