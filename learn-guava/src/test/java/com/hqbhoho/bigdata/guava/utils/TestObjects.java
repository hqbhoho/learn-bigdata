package com.hqbhoho.bigdata.guava.utils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * describe:
 *
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/05
 */
public class TestObjects {
    @Test
    public void test_compare_to() {
        Person person1 = new Person(1, "hqbhoho", 26);
        Person person2 = new Person(2, "xiaomixiu", 27);
        Person person3 = new Person(2, "xiaoxiao", 25);
        List<Person> persons = Arrays.asList(person1, person2, person3);
        Collections.sort(persons);
        Optional.ofNullable(Arrays.toString(persons.toArray())).ifPresent(System.out::println);

    }

    /**
     * 使用Guava重写类里面的一些方法
     */
    static class Person implements Comparable<Person> {
        private int groupId;
        private String name;
        private int age;

        public Person(int groupId, String name, int age) {
            this.groupId = groupId;
            this.name = name;
            this.age = age;
        }

        public int getGroupId() {
            return groupId;
        }

        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Person person = (Person) o;
            return groupId == person.groupId &&
                    age == person.age &&
                    Objects.equal(name, person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(groupId, name, age);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("groupId", groupId)
                    .add("name", name)
                    .add("age", age)
                    .toString();
        }

        @Override
        /**
         *
         * 使用guava的compare方法
         */
        public int compareTo(Person person) {
            return ComparisonChain
                    .start()
                    .compare(this.groupId, person.groupId)
                    .compare(this.age, person.age)
                    .result();
        }
    }
}
