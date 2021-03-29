package com.github.mihaibogdaneugen.redisrepository;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

interface HashRedisRepository<T, SerializationType> extends HashClusterRedisRepository<T, SerializationType> {

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
     * Retrieve all keys of all entities in the current collection.
     * @return Set of String objects representing entity identifiers
     */
    Set<String> getAllKeys();
}
