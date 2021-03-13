package com.github.mihaibogdaneugen.redisrepository;

import java.util.List;
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
     * @param ids The array of Strings identifiers of entities
     * @return A list of entities
     */
    List<T> get(final String... ids);

    /**
     * Retrieves all entities from the current collection.<br/>
     * @return A list of entities
     */
    List<T> getAll();

    /**
     * Checks if the entity with the specified identifier exists in the repository or not.<br/>
     * @param id The String identifier of the entity
     * @return A Boolean object, true if it exists, false otherwise
     */
    Boolean exists(final String id);

    /**
     * Replaces (or inserts) the given entity with the specified identifier.<br/>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void set(final String id, final T entity);

    /**
     * Inserts the given entity with the specified identifier, only if it does not exist.<br/>
     * @param id The String identifier of the entity
     * @param entity The entity to be set
     */
    void setIfNotExist(final String id, final T entity);

    /**
     * Removes the entity with the given identifier.<br/>
     * @param id The String identifier of the entity
     */
    void delete(final String id);

    /**
     * Removes all entities with the given identifiers.<br/>
     * @param ids The array of Strings identifiers of entities
     */
    void delete(final String... ids);

    /**
     * Removes all entities from the current collection.<br/>
     */
    void deleteAll();

    /**
     * Retrieve all keys of all entities in the current collection.
     * @return Set of String objects representing entity identifiers
     */
    Set<String> getAllKeys();
}
