package com.github.mihaibogdaneugen.redisrepository;

import java.util.Optional;

interface ValueClusterRedisRepository<T, SerializationType> {

    SerializationType convertTo(final T entity);

    T convertFrom(final SerializationType value);

    /**
     * Retrieves the entity with the given identifier.
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    Optional<T> get(final String id);

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    Boolean exists(final String id);

    /**
     * Sets (updates or inserts) the given entity with the specified identifier.
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void set(final String id, final T entity);

    /**
     * Sets the given entity with the specified identifier only if it does exist (update).
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfItExists(final String id, final T entity);

    /**
     * Sets the given entity with the specified identifier only if it does not exist (insert).
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfDoesNotExist(final String id, final T entity);

    /**
     * Removes the entity with the given identifier.
     * @param id The String identifier of the entity
     */
    void delete(final String id);

    /**
     * Sets the expiration after the given number of milliseconds for the entity with the given identifier.
     * @param id The String identifier of the entity
     * @param milliseconds The number of milliseconds after which the entity will expire
     */
    void setExpirationAfter(final String id, final long milliseconds);

    /**
     * Sets the expiration at the given timestamp (Unix time) for the entity with the given identifier.
     * @param id The String identifier of the entity
     * @param millisecondsTimestamp The timestamp (Unix time) when the entity will expire
     */
    void setExpirationAt(final String id, final long millisecondsTimestamp);

    /**
     * Returns the time to live left in milliseconds till the entity will expire.
     * @param id The String identifier of the entity
     * @return No. of milliseconds
     */
    Long getTimeToLiveLeft(final String id);

    /**
     * Lua Script for updating an entity only if it exists and its value is equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptUpdateIfItIs() {
        return "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('set', KEYS[1], ARGV[2]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Lua Script for updating an entity only if it exists and its value is not equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptUpdateIfItIsNot() {
        return "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) ~= ARGV[1] then " +
                "return redis.call('set', KEYS[1], ARGV[2]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Lua Script for deleting an entity only if it exists and its value is equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptDeleteIfItIs() {
        return "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) == ARGV[1] then " +
                "return redis.call('del', KEYS[1]); " +
                "end; " +
                "return 0;";
    }

    /**
     * Lua Script for deleting an entity only if it exists and its value is not equal with the provided one.
     * @return String representing the Lua script
     */
    default String getLuaScriptDeleteIfItIsNot() {
        return "if redis.call('exists', KEYS[1]) == 1 and redis.call('get', KEYS[1]) ~= ARGV[1] then " +
                "return redis.call('del', KEYS[1]); " +
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
