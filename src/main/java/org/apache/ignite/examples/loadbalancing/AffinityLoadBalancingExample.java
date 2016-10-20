package org.apache.ignite.examples.loadbalancing;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.examples.loadbalancing.Company.CompanyKey;
import org.apache.ignite.examples.loadbalancing.Person.PersonKey;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

public class AffinityLoadBalancingExample {
    private static final String COMPANY_KEY = "company #";
    private static final String PERSON_KEY = "person #";

    public static void main(final String[] args) {
        startGridNodes();

        final IgniteConfiguration cfg = createIgniteConfiguration();

        cfg.setGridName("affinity-grid-node-client");
        cfg.setClientMode(true);

        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println();
            System.out.println(">>> Affinity key mapper example started.");

            final Map<CompanyKey, Company> companies = createCompanies();
            final Map<PersonKey, Person> persons = createPersons(companies.values());

            initCache(ignite, Company.CACHE_NAME, companies);
            initCache(ignite, Person.CACHE_NAME, persons);

            runAffinityJobs(ignite, companies.values());

            destroyCaches(ignite);
        }

        stopGridNodes();
    }

    private static void runAffinityJobs(final Ignite ignite, final Collection<Company> companies) {
        final IgniteCompute compute = ignite.compute();

        final Map<Company, BigDecimal> result = compute.execute(new AverageSalaryInCompanyComputeTask(),
                new AffinityComputeTaskInfo<>(Company.CACHE_NAME, companies));

        printResult(result);
    }

    private static void printResult(final Map<Company, BigDecimal> result) {
        System.out.println("------------------------------------");
        System.out.println("| Company name | emploee avg salary |");
        System.out.println("------------------------------------");

        for (final Entry<Company, BigDecimal> resultEntry : result.entrySet()) {
            final String companyName = resultEntry.getKey().getCompanyName();
            final String avgSalary = resultEntry.getValue().toString();

            System.out.println("|   " + companyName + "   |      " + avgSalary + "      |");
        }

        System.out.println("-------------------------------------");
    }

    private static void destroyCaches(final Ignite ignite) {
        // Distributed cache could be removed from cluster only by
        // #destroyCache() call.
        ignite.destroyCache(Person.CACHE_NAME);
        ignite.destroyCache(Company.CACHE_NAME);
    }

    private static <K, V> void initCache(final Ignite ignite, final String cacheName, final Map<K, V> values) {
        try (IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName)) {
            for (final Map.Entry<K, V> entry : values.entrySet()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private static IgniteConfiguration createIgniteConfiguration() {
        final IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPeerClassLoadingEnabled(true);

        cfg.setCacheConfiguration(createCacheConfiguration(Company.CACHE_NAME),
                createCacheConfiguration(Person.CACHE_NAME));
        cfg.setLoadBalancingSpi(new AffinityLoadBalancingSpi());
        cfg.setDiscoverySpi(createDiscoverySpi());

        return cfg;
    }

    private static CacheConfiguration<String, Object> createCacheConfiguration(final String cacheName) {
        final CacheConfiguration<String, Object> cacheCfg = new CacheConfiguration<>(cacheName);

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setStatisticsEnabled(true);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        return cacheCfg;
    }

    private static DiscoverySpi createDiscoverySpi() {
        final TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();

        final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500..47509"));

        discoverySpi.setIpFinder(ipFinder);

        return discoverySpi;
    }

    private static Map<CompanyKey, Company> createCompanies() {
        final Map<CompanyKey, Company> companies = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            final CompanyKey key = new CompanyKey(COMPANY_KEY + i);
            final Company company = new Company(key);
            company.setCompanyName("Company #" + i);

            companies.put(key, company);
        }

        return companies;
    }

    private static Map<PersonKey, Person> createPersons(final Collection<Company> companies) {
        final Map<PersonKey, Person> persons = new HashMap<>();

        for (final Company company : companies) {
            System.out.println(String.format("Staff of the company %s", company.getCompanyName()));

            final int personCount = ThreadLocalRandom.current().nextInt(1, 11);

            for (int i = 0; i < personCount; i++) {
                final PersonKey key = new PersonKey(PERSON_KEY + i, company.getKey().getCompanyId());
                final Person person = new Person(key);
                person.setSalary(BigDecimal.valueOf(ThreadLocalRandom.current().nextInt(100_000, 200_000)));

                persons.put(key, person);

                System.out.println(String.format("%s with salary %s", person.getKey().getPersonId(),
                        person.getSalary().toString()));
            }

            System.out.println("------------------------------");
        }

        return persons;
    }

    private static void startGridNodes() {
        for (int i = 0; i < 3; i++) {
            final IgniteConfiguration cfg = createIgniteConfiguration();

            cfg.setGridName("affinity-grid-node-" + i);

            Ignition.start(cfg);
        }
    }

    private static void stopGridNodes() {
        Ignition.stopAll(true);
    }
}
