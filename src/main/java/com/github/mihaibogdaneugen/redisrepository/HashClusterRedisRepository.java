package com.github.mihaibogdaneugen.redisrepository;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

interface HashClusterRedisRepository<T, SerializationType> {

    Map<SerializationType, SerializationType> convertTo(final T entity);

    T convertFrom(final Map<SerializationType, SerializationType> value);

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
     * Removes the entity with the given identifier.
     * @param id The String identifier of the entity
     */
    void delete(final String id);

    /**
     * Replaces (or inserts) a specific field of an entity identified by the given identifier.
     * @param id The String identifier of the entity
     * @param fieldAndValue A Map.Entry pair of a field and value, serialized as String objects
     */
    void setField(final String id, final Map.Entry<SerializationType, SerializationType> fieldAndValue);

    /**
     * Inserts a specific field of an entity identified by the given identifier, only if it does not exist.
     * @param id The String identifier of the entity
     * @param fieldAndValue A Map.Entry pair of a field and value, serialized as String objects
     */
    void setFieldIfNotExists(final String id, final Map.Entry<SerializationType, SerializationType> fieldAndValue);

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
}
