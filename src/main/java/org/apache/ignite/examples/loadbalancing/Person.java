package org.apache.ignite.examples.loadbalancing;

import java.math.BigDecimal;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class Person {
    public static final String CACHE_NAME = "personCache";

    private final PersonKey key;

    private String firstName;
    private String lastName;

    private BigDecimal salary;

    public Person(final PersonKey key) {
        this.key = key;
    }

    public PersonKey getKey() {
        return key;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(final String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(final String lastName) {
        this.lastName = lastName;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(final BigDecimal salary) {
        this.salary = salary;
    }

    public static class PersonKey {
        private final String personId;
        @AffinityKeyMapped
        private final String companyId;

        public PersonKey(final String personId, final String companyId) {
            this.personId = personId;
            this.companyId = companyId;
        }

        public String getPersonId() {
            return personId;
        }

        public String getCompanyId() {
            return companyId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((companyId == null) ? 0 : companyId.hashCode());
            result = prime * result + ((personId == null) ? 0 : personId.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final PersonKey other = (PersonKey) obj;
            if (companyId == null) {
                if (other.companyId != null)
                    return false;
            } else if (!companyId.equals(other.companyId))
                return false;
            if (personId == null) {
                if (other.personId != null)
                    return false;
            } else if (!personId.equals(other.personId))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "PersonKey [personId=" + personId + ", companyId=" + companyId + "]";
        }
    }
}
