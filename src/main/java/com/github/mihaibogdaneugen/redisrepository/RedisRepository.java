package com.github.mihaibogdaneugen.redisrepository;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface RedisRepository<T> {

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
     * Sets the given entity with the specified identifier only if it does exist (update).
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfItDoesExist(final String id, final T entity);

    /**
     * Sets the given entity with the specified identifier only if it does not exist (insert).
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfItDoesNotExist(final String id, final T entity);

    /**
     * Updates the entity with the specified identifier by calling the `updater` function.
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    Optional<Boolean> update(final String id, final Function<T, T> updater);

    /**
     * Updates the entity with the specified identifier by calling the `updater` function only if the condition returns true (conditional update).
     * @param id The String identifier of the entity
     * @param updater A function that updates the entity
     * @param condition A function that represents the condition for the update to happen
     * @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    Optional<Boolean> update(final String id, final Function<T, T> updater, final Function<T, Boolean> condition);

    /**
     * Removes the entity with the given identifier.<br/>
     * @param id The String identifier of the entity
     */
    void delete(final String id);

    /**
     * Removes the entity with the given identifier only if the condition returns true (conditional delete).
     * @param id The String identifier of the entity
     * @param condition A function that represents the condition for the delete to happen
     @return Optional object, empty if no such entity exists, or boolean value indicating the status of the transaction
     */
    Optional<Boolean> delete(final String id, final Function<T, Boolean> condition);

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
    Set<String> getAllIds();

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
