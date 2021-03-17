package com.github.mihaibogdaneugen.redisrepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

interface ValueRedisRepository<T, SerializationType> {

    SerializationType convertTo(final T entity);

    T convertFrom(final SerializationType value);

    /**
     * Retrieves the entity with the given identifier.
     * @param id The String identifier of the entity
     * @return Optional object, empty if no such entity is found, or the object otherwise
     */
    Optional<T> get(final String id);

    /**
     * Retrieves the entities with the given identifiers.
     * @param ids The array of Strings identifiers of entities
     * @return A list of entities
     */
    List<T> get(final String... ids);

    /**
     * Retrieves all entities from the current collection.
     * @return A list of entities
     */
    List<T> getAll();

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    Boolean exists(final String id);

    /**
     * Replaces (or inserts) the given entity with the specified identifier.
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void set(final String id, final T entity);

    /**
     * Inserts the given entity with the specified identifier, only if it does exist.
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfExist(final String id, final T entity);

    /**
     * Inserts the given entity with the specified identifier, only if it does not exist.
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfNotExist(final String id, final T entity);

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
     * @param ids The array of Strings identifiers of entities
     */
    void delete(final String... ids);

    /**
     * Removes all entities from the current collection.
     */
    void deleteAll();

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
