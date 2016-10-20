package org.apache.ignite.examples.loadbalancing;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

public class Company {
    public static final String CACHE_NAME = "companyCache";

    private final CompanyKey key;

    private String companyName;

    public Company(final CompanyKey key) {
        this.key = key;
    }

    public CompanyKey getKey() {
        return key;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(final String companyName) {
        this.companyName = companyName;
    }

    public static class CompanyKey {
        @AffinityKeyMapped
        private final String companyId;

        public CompanyKey(final String companyId) {
            this.companyId = companyId;
        }

        public String getCompanyId() {
            return companyId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((companyId == null) ? 0 : companyId.hashCode());
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
            final CompanyKey other = (CompanyKey) obj;
            if (companyId == null) {
                if (other.companyId != null)
                    return false;
            } else if (!companyId.equals(other.companyId))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "CompanyKey [companyId=" + companyId + "]";
        }
    }
}
