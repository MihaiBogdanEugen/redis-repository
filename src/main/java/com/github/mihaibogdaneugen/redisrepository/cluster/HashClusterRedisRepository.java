package com.github.mihaibogdaneugen.redisrepository.cluster;

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
     * Retrieves the entities with the given identifiers.
     * @param ids The set of Strings identifiers of entities
     * @return A set of entities
     */
    Set<T> get(final Set<String> ids);

    /**
     * Retrieves all entities from the current collection.
     * @return A set of entities
     */
    Set<T> getAll();

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
     * Updates the entity with the specified identifier by calling the `updater` function.
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    Optional<Boolean> update(final String id, final Function<T, T> updater);

    /**
     * Updates the entity with the specified identifier by calling the `updater` function only if the `conditioner` returns true (conditional update).
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @param conditioner A function that represents the condition for the update to happen
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    Optional<Boolean> update(final String id, final Function<T, T> updater, final Function<T, Boolean> conditioner);

    /**
     * Removes the entity with the given identifier.
     * @param id The String identifier of the entity
     */
    void delete(final String id);

    /**
     * Removes the entity with the given identifier only if the `conditioner` returns true (conditional delete).
     * @param id The String identifier of the entity
     * @param conditioner A function that represents the condition for the delete to happen
     @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    Optional<Boolean> delete(final String id, final Function<T, Boolean> conditioner);

    /**
     * Removes all entities with the given identifiers.
     * @param ids The set of Strings identifiers of entities
     */
    void delete(final Set<String> ids);

    /**
     * Removes all entities from the current collection.
     */
    void deleteAll();

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

    /**
     * Retrieve all keys of all entities in the current collection.
     * @return Set of String objects representing entity identifiers
     */
    Set<String> getAllKeys();
}
