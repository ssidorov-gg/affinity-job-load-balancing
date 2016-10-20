package org.apache.ignite.examples.loadbalancing;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.loadbalancing.LoadBalancingSpi;


@IgniteSpiMultipleInstancesSupport(true)
public class AffinityLoadBalancingSpi extends IgniteSpiAdapter implements LoadBalancingSpi {
    @IgniteInstanceResource
    private Ignite ignite;

    @Override
    public void spiStart(final String gridName) throws IgniteSpiException {
        // no-op
    }

    @Override
    public void spiStop() throws IgniteSpiException {
        // no-op
    }

    @Override
    public ClusterNode getBalancedNode(final ComputeTaskSession ses, final List<ClusterNode> top, final ComputeJob job)
            throws IgniteException {
        final Object jobAffinityKey = getAffinityKey(job);

        final String cacheName = ses.getAttribute(AffinityComputeTaskInfo.CACHE_NAME_SESS_ATTRIBUTE_KEY);

        final ClusterNode node = ignite.affinity(cacheName).mapKeyToNode(jobAffinityKey);

        System.out.println(String.format("execute job with key %s on node %s", jobAffinityKey, node.id()));

        return node;
    }

    private Object getAffinityKey(final ComputeJob job) {
        final Field[] fields = job.getClass().getDeclaredFields();

        for (final Field field : fields) {
            if (field.isAnnotationPresent(AffinityKeyMapped.class)) {
                try {
                    field.setAccessible(true);
                    return field.get(job);
                } catch (final IllegalAccessException e) {
                    throw new IllegalArgumentException("Affinity key value cannot be retreived", e);
                }
            }
        }

        throw new IllegalArgumentException("Affinity key not found");
    }
}
