package com.github.mihaibogdaneugen.redisrepository;

import java.util.Optional;
import java.util.Set;

interface HashValueRedisRepository<T, SerializationType> {

    SerializationType convertTo(final T entity);

    T convertFrom(final SerializationType value);

    /**
     * Retrieves the entity with the given identifier.<br/>
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    Optional<T> get(final String id);

    /**
     * Retrieves the entities with the given identifiers.<br/>
     * @param ids The set of Strings identifiers of entities
     * @return A set of entities
     */
    Set<T> get(final Set<String> ids);

    /**
     * Retrieves all entities from the current collection.<br/>
     * @return A set of entities
     */
    Set<T> getAll();

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.<br/>
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    Boolean exists(final String id);

    /**
     * Sets (updates or inserts) the given entity with the specified identifier.<br/>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void set(final String id, final T entity);

    /**
     * Sets the given entity with the specified identifier only if it does not exist (insert).<br/>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfDoesNotExist(final String id, final T entity);

    /**
     * Removes the entity with the given identifier.<br/>
     * @param id The String identifier of the entity
     */
    void delete(final String id);

    /**
     * Removes all entities with the given identifiers.<br/>
     * @param ids The set of Strings identifiers of entities
     */
    void delete(final Set<String> ids);

    /**
     * Removes all entities from the current collection.<br/>
     */
    void deleteAll();

    /**
     * Retrieve all keys of all entities in the current collection.
     * @return Set of String objects representing entity identifiers
     */
    Set<String> getAllKeys();

    /**
     * Lua Script for updating an entity only if it exists and its value is equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptUpdateIfItIs() {
        return "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) == ARGV[1] then " +
                "return redis.call('hset', KEYS[1], KEYS[2], ARGV[2]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Lua Script for updating an entity only if it exists and its value is not equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptUpdateIfItIsNot() {
        return "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) ~= ARGV[1] then " +
                "return redis.call('hset', KEYS[1], KEYS[2], ARGV[2]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Lua Script for deleting an entity only if it exists and its value is equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptDeleteIfItIs() {
        return "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) == ARGV[1] then " +
                "return redis.call('hdel', KEYS[1], KEYS[2]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Lua Script for deleting an entity only if it exists and its value is not equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptDeleteIfItIsNot() {
        return "if redis.call('hexists', KEYS[1], KEYS[2]) == 1 and redis.call('hget', KEYS[1], KEYS[2]) ~= ARGV[1] then " +
                "return redis.call('hdel', KEYS[1], KEYS[2]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Update the entity identified by the given identifier to the new provided value if its old value is equal with the given one.
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     * @param newValue The new value of the entity
     */
    void updateIfItIs(final String id, final T oldValue, final T newValue);

    /**
     * Update the entity identified by the given identifier to the new provided value if its old value is not equal with the given one.
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     * @param newValue The new value of the entity
     */
    void updateIfItIsNot(final String id, final T oldValue, final T newValue);

    /**
     * Delete the entity identified by the given identifier if its old value is equal with the given one.
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     */
    void deleteIfItIs(final String id, final T oldValue);

    /**
     * Delete the entity identified by the given identifier if its old value is not equal with the given one.
     * @param id The String identifier of the entity
     * @param oldValue The old value of the entity
     */
    void deleteIfItIsNot(final String id, final T oldValue);
}
