package com.github.mihaibogdaneugen.redisrepository;

import java.time.LocalDate;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

public class Person {

    public enum EyeColor {
        UNKNOWN,
        BLACK,
        BLUE,
        GREY,
        BROWN,
        GREEN
    }

    private String id;
    private String fullName;
    private LocalDate dateOfBirth;
    private boolean isMarried;
    private float heightMeters;
    private EyeColor eyeColor;

    public Person() { }

    public Person(
            final String id,
            final String fullName,
            final LocalDate dateOfBirth,
            final boolean isMarried,
            final float heightMeters,
            final EyeColor eyeColor) {
        this.id = id;
        this.fullName = fullName;
        this.dateOfBirth = dateOfBirth;
        this.isMarried = isMarried;
        this.heightMeters = heightMeters;
        this.eyeColor = eyeColor;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(final String fullName) {
        this.fullName = fullName;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }

    public void setDateOfBirth(final LocalDate dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public boolean isMarried() {
        return isMarried;
    }

    public void setMarried(final boolean married) {
        isMarried = married;
    }

    public float getHeightMeters() {
        return heightMeters;
    }

    public void setHeightMeters(final float heightMeters) {
        this.heightMeters = heightMeters;
    }

    public EyeColor getEyeColor() {
        return eyeColor;
    }

    public void setEyeColor(final EyeColor eyeColor) {
        this.eyeColor = eyeColor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Person person = (Person) o;
        return isMarried == person.isMarried
                && Float.compare(person.heightMeters, heightMeters) == 0
                && Objects.equals(id, person.id)
                && Objects.equals(fullName, person.fullName)
                && Objects.equals(dateOfBirth, person.dateOfBirth)
                && eyeColor == person.eyeColor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id, fullName, dateOfBirth, isMarried, heightMeters, eyeColor);
    }

    public static Person random() {
        final var random = new Random();
        final var id = UUID.randomUUID().toString();
        final var fullName = UUID.randomUUID().toString() + " " + UUID.randomUUID().toString();
        final var dateOfBirth = LocalDate.of(1950 + random.nextInt(30), 1 + random.nextInt(12), 1 + random.nextInt(28));
        final var isMarried = random.nextInt() % 2 == 0;
        final var heightMeters = (150 + random.nextInt(50)) / 100f;
        final var eyeColor = EyeColor.values()[random.nextInt(EyeColor.values().length)];
        return new Person(id, fullName, dateOfBirth, isMarried, heightMeters, eyeColor);
    }
}
