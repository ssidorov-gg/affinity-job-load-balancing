package org.apache.ignite.examples.loadbalancing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.examples.loadbalancing.Company.CompanyKey;
import org.apache.ignite.examples.loadbalancing.Person.PersonKey;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskSessionResource;

@SuppressWarnings("serial")
@ComputeTaskSessionFullSupport
public class AverageSalaryInCompanyComputeTask
        extends ComputeTaskSplitAdapter<AffinityComputeTaskInfo<Collection<Company>>, Map<Company, BigDecimal>> {

    @TaskSessionResource
    private ComputeTaskSession ses;

    @Override
    protected Collection<? extends ComputeJob> split(final int gridSize,
            final AffinityComputeTaskInfo<Collection<Company>> taskInfo) throws IgniteException {

        ses.setAttribute(AffinityComputeTaskInfo.CACHE_NAME_SESS_ATTRIBUTE_KEY,  taskInfo.getCacheName());

        final Collection<Company> companies = taskInfo.getArgs();

        final Collection<ComputeJob> jobs = new ArrayList<>();

        for (final Company company : companies) {
            jobs.add(new ComputeAverageSalaryInCompanyJob(company));
        }

        return jobs;
    }

    @Override
    public Map<Company, BigDecimal> reduce(final List<ComputeJobResult> results) throws IgniteException {
        final Map<Company, BigDecimal> avgSalaryByCompany = new HashMap<>();

        for (final ComputeJobResult result : results) {
            final ComputeAverageSalaryInCompanyJob job = result.getJob();
            final BigDecimal avgSalary = result.getData();

            avgSalaryByCompany.put(job.company, avgSalary);
        }

        return avgSalaryByCompany;
    }

    private class ComputeAverageSalaryInCompanyJob extends ComputeJobAdapter {
        @IgniteInstanceResource
        private Ignite ignite;

        private Company company;

        @AffinityKeyMapped
        private CompanyKey companyKey;

        private IgniteBiPredicate<PersonKey, Person> filter;

        public ComputeAverageSalaryInCompanyJob(final Company company) {
            this.company = company;
            this.companyKey = company.getKey();

            filter = new IgniteBiPredicate<PersonKey, Person>() {
                @Override
                public boolean apply(final PersonKey key, final Person person) {
                    return key.getCompanyId().equals(companyKey.getCompanyId());
                }
            };
        }

        @Override
        public Object execute() throws IgniteException {
            System.out.println(String.format("Calculating avg salary for company %s has been started on grid %s [%s]",
                    companyKey.getCompanyId(), ignite.name(), ignite.cluster().localNode().id()));

            try (IgniteCache<PersonKey, Person> cache = ignite.getOrCreateCache(Person.CACHE_NAME)) {

                final Query<Entry<PersonKey, Person>> employeesQuery = new ScanQuery<>(filter);
                employeesQuery.setLocal(true);

                BigDecimal totalSalary = BigDecimal.ZERO;
                int empCount = 0;

                try (QueryCursor<Entry<PersonKey, Person>> cursor = cache.query(employeesQuery)) {
                    for (final Entry<PersonKey, Person> entry : cursor) {
                        totalSalary = totalSalary.add(entry.getValue().getSalary());
                        empCount++;
                    }
                }

                return totalSalary.divide(BigDecimal.valueOf(empCount), RoundingMode.HALF_UP);
            }
        }
    }
}
