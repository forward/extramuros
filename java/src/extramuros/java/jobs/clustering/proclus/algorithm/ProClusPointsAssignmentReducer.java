package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 16:17
 */
public class ProClusPointsAssignmentReducer extends Reducer<Cluster,VectorWritable,Text,Cluster> {

    private static final Logger log = LoggerFactory.getLogger(ProClusPointsAssignmentReducer.class);

    @Override
    protected void reduce(Cluster cluster, Iterable<VectorWritable> values, Context context) {

        Vector centroid = null;
        int counter = 0;

        for(VectorWritable writable : values) {
            counter++;
            Vector vector = writable.get();

            if(centroid==null) {
                centroid = vector;
            } else {
                vector.addTo(centroid);
            }
        }

        centroid.divide(counter);

        log.info("*** WRITING CLUSTER: "+cluster.getLabel());
        log.info("WRITING CENTROID: "+centroid);
        log.info("CLUSTERED POINTS: "+counter);
        cluster.setCentroid(centroid);
        cluster.setNumPoints(counter);

        try {
            context.write(new Text(cluster.getMedoid().getLabel()), cluster);
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
