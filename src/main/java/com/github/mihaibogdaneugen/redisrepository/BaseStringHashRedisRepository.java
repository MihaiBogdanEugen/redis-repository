package com.github.mihaibogdaneugen.redisrepository;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A RedisRepository for a specified entity type, where all entities are serialized as maps of <br/>
 * UTF-8 encoded Strings and stored as key-maps pairs (Redis hashes) <br/>
 * Design details:<br/>
 * - every entity has a String identifier, but this is not enforced as part of the type itself.<br/>
 * - every entity is part of a collection that groups all entities with the same type<br/>
 * - the actual key of a given entity is composed as the `${collection_key}:${entity_id}`<br/>
 * Note: This repository optimises for simple and transactional `insert`, `update`, `delete_one`, `get_one`,<br/>
 * operations, but it's not recommended for `get_some`, `delete_some`, `get_all` or `delete_all` operations. <br/>
 * These last four operations are not implemented in a transactional manner and they're highly inefficient <br/>
 * for large collections.
 * @param <T> The type of the entity
 */
public abstract class BaseStringHashRedisRepository<T>
        extends RedisRepository
        implements StringHashRedisRepository<T> {

    private final String keyPrefix;
    private final String allKeysPattern;

    /**
     * Builds a BaseStringHashRedisRepository, based around a Jedis object, for a specific collection.<br/>
     * The provided Jedis object will be closed should `.close()` be called.
     * @param jedis The Jedis object
     * @param collectionKey The name (key) of the collection
     */
    public BaseStringHashRedisRepository(final Jedis jedis, final String collectionKey) {
        super(jedis);
        throwIfNullOrEmptyOrBlank(collectionKey, "collectionKey");
        if (collectionKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Collection key `" + collectionKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`");
        }
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = collectionKey + DEFAULT_KEY_SEPARATOR + "*";
    }

    /**
     * Builds a BaseStringHashRedisRepository, based around a jedisPool object, for a specific collection.<br/>
     * A Jedis object will be retrieved from the JedisPool by calling `.getResource()` and it will<br/>
     * be closed should `.close()` be called.
     * @param jedisPool The JedisPool object
     * @param collectionKey The name (key) of the collection
     */
    public BaseStringHashRedisRepository(final JedisPool jedisPool, final String collectionKey) {
        super(jedisPool);
        throwIfNullOrEmptyOrBlank(collectionKey, "collectionKey");
        if (collectionKey.contains(DEFAULT_KEY_SEPARATOR)) {
            throw new IllegalArgumentException("Collection key `" + collectionKey + "` cannot contain `" + DEFAULT_KEY_SEPARATOR + "`");
        }
        keyPrefix = collectionKey + DEFAULT_KEY_SEPARATOR;
        allKeysPattern = collectionKey + DEFAULT_KEY_SEPARATOR + "*";
    }

//    /**
//     * Converts the given entity to a Map of Strings.
//     * @param entity The entity to be converted
//     * @return A Map of Strings object
//     */
//    public abstract Map<String, String> convertTo(final T entity);
//
//    /**
//     * Converts back the given Map of Strings to an entity.
//     * @param entityAsMap The Map of Strings representation of the entity
//     * @return An entity object
//     */
//    public abstract T convertFrom(final Map<String, String> entityAsMap);

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
        final var entity = jedis.hgetAll(key);
        return isNullOrEmpty(entity)
                ? Optional.empty()
                : Optional.of(convertFrom(entity));
    }

    /**
     * Retrieves the entities with the given identifiers.<br/>
     * This method will do a batch-get for all entities with the given identifiers.<br/>
     * Warning: Do not use this method with large databases due to poor performance <br/>
     * of PIPELINED HGETALLs Redis commands.<br/>
     * Note: This method calls the KEYS, PIPELINED, HGETALL and SYNC Redis commands.
     * @see <a href="https://redis.io/commands/KEYS">KEYS</a>
     * @see <a href="https://redis.io/commands/PIPELINED">PIPELINED</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/SYNC">SYNC</a>
     * @param ids The array of Strings identifiers of entities
     * @return A list of entities
     */
    @Override
    public final List<T> get(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var keys = getKeys(ids);
        return getByKeys(keys);
    }

    /**
     * Retrieves all entities from the current collection.<br/>
     * This method first retrieves all keys first and then all entities, as a batch-get<br/>
     * but it does not provide any transactional behaviour. <br/>
     * Warning: Do not use this method with large databases due to very poor performance <br/>
     * of KEYS and PIPELINED HGETALLs Redis commands.<br/>
     * Note: This method calls the KEYS, PIPELINED, HGETALL and SYNC Redis commands.
     * @see <a href="https://redis.io/commands/KEYS">KEYS</a>
     * @see <a href="https://redis.io/commands/PIPELINED">PIPELINED</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/SYNC">SYNC</a>
     * @return A list of entities
     */
    @Override
    public final List<T> getAll() {
        final var keys = getAllKeys();
        return getByKeys(keys);
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
        return jedis.exists(key);
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
        final var key = getKey(id);
        jedis.hset(key, convertTo(entity));
    }

    @Override
    public final void setIfExist(final String id, final T entity) {

    }

    /**
     * Inserts the given entity with the specified identifier, only if it does not exist.<br/>
     * This method works in a transactional manner by watching for such a key.<br/>
     * Note: This method calls the WATCH, EXISTS, UNWATCH, MULTI, HSET and EXEC Redis command.
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/EXISTS">EXISTS</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    @Override
    public final void setIfNotExist(final String id, final T entity) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(entity, "entity");
        final var key = getKey(id);
        jedis.watch(key);
        if (jedis.exists(key)) {
            jedis.unwatch();
            return;
        }
        try (final var transaction = jedis.multi()) {
            transaction.hset(key, convertTo(entity));
            transaction.exec();
        }
    }

    /**
     * Updates the entity with the specified identifier by calling the `updater` function.<br/>
     * This method provides a transactional behaviour for updating the entity.<br/>
     * Note: This method calls the WATCH, HGETALL, UNWATCH, MULTI, HSET and EXEC Redis commands.
     * @see <a href="https://redis.io/commands/WATCH">WATCH</a>
     * @see <a href="https://redis.io/commands/HGETALL">HGETALL</a>
     * @see <a href="https://redis.io/commands/UNWATCH">UNWATCH</a>
     * @see <a href="https://redis.io/commands/MULTI">MULTI</a>
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @see <a href="https://redis.io/commands/EXEC">EXEC</a>
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(updater, "updater");
        final var key = getKey(id);
        jedis.watch(key);
        final var value = jedis.hgetAll(key);
        if (isNullOrEmpty(value)) {
            jedis.unwatch();
            return Optional.empty();
        }
        final var entity = convertFrom(value);
        final var newEntity = updater.apply(entity);
        final var newValue = convertTo(newEntity);
        final List<Object> results;
        try (final var transaction = jedis.multi()) {
            transaction.hset(key, newValue);
            results = transaction.exec();
        }
        return Optional.of(isNotNullNorEmpty(results));
    }

    @Override
    public final Optional<Boolean> update(final String id, final Function<T, T> updater, final Function<T, Boolean> conditioner) {
        return Optional.empty();
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
        jedis.del(key);
    }

    /**
     * Removes all entities with the given identifiers.<br/>
     * Note: This method calls the DEL Redis command.
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     * @param ids The array of Strings identifiers of entities
     */
    @Override
    public final void delete(final String... ids) {
        throwIfNullOrEmpty(ids);
        final var keys = getKeys(ids);
        jedis.del(keys);
    }

    /**
     * Removes all entities from the current collection.<br/>
     * This method first retrieves all keys first and then deletes all entities, <br/>
     * but it does not provide any transactional behaviour. <br/>
     * Warning: Do not use this method with large databases due to poor performance of KEYS Redis command.<br/>
     * Note: This method calls the KEYS and DEL Redis commands.
     * @see <a href="https://redis.io/commands/KEYS">KEYS</a>
     * @see <a href="https://redis.io/commands/DEL">DEL</a>
     */
    @Override
    public final void deleteAll() {
        final var keys = getAllKeys();
        jedis.del(keys);
    }

    /**
     * Replaces (or inserts) a specific field of an entity identified by the given identifier.<br/>
     * Note: This method calls the HSET Redis command.
     * @see <a href="https://redis.io/commands/HSET">HSET</a>
     * @param id The String identifier of the entity
     * @param fieldAndValue A Map.Entry pair of a field and value, serialized as String objects
     */
    public final void setField(final String id, final Map.Entry<String, String> fieldAndValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(fieldAndValue, "fieldAndValue");
        final var key = getKey(id);
        jedis.hset(key, fieldAndValue.getKey(), fieldAndValue.getValue());
    }

    /**
     * Inserts a specific field of an entity identified by the given identifier, only if it does not exist.<br/>
     * Note: This method calls the HSETNX Redis command.
     * @see <a href="https://redis.io/commands/HSETNX">HSETNX</a>
     * @param id The String identifier of the entity
     * @param fieldAndValue A Map.Entry pair of a field and value, serialized as String objects
     */
    public final void setFieldIfNotExists(final String id, final Map.Entry<String, String> fieldAndValue) {
        throwIfNullOrEmptyOrBlank(id, "id");
        throwIfNull(fieldAndValue, "fieldAndValue");
        final var key = getKey(id);
        jedis.hsetnx(key, fieldAndValue.getKey(), fieldAndValue.getValue());
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
        jedis.pexpire(key, milliseconds);
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
        jedis.pexpireAt(key, millisecondsTimestamp);
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
        return jedis.pttl(key);
    }

    private List<T> getByKeys(final String... keys) {
        final var responses = new ArrayList<Response<Map<String, String>>>();
        try (final var pipeline = jedis.pipelined()) {
            Arrays.stream(keys).forEach(key -> responses.add(pipeline.hgetAll(key)));
            pipeline.sync();
        }
        return responses.stream()
                .map(Response::get)
                .filter(RedisRepository::isNotNullNorEmpty)
                .map(this::convertFrom)
                .collect(Collectors.toList());
    }

    private String[] getKeys(final String... keySuffixes) {
        return Arrays.stream(keySuffixes)
                .filter(RedisRepository::isNotNullNorEmptyNorBlank)
                .map(this::getKey)
                .toArray(String[]::new);
    }

    private String getKey(final String keySuffix) {
        return keyPrefix + keySuffix;
    }

    private String[] getAllKeys() {
        return jedis.keys(allKeysPattern).toArray(String[]::new);
    }
}