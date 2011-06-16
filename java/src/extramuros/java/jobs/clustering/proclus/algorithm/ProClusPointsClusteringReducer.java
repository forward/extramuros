package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 16:17
 */
public class ProClusPointsClusteringReducer extends Reducer<Cluster,VectorWritable,Text,VectorWritable> {

    private static final Logger log = LoggerFactory.getLogger(ProClusPointsClusteringReducer.class);

    @Override
    protected void reduce(Cluster cluster, Iterable<VectorWritable> values, Context context) {


        for(VectorWritable writable : values) {
            try {
                if(cluster.getMedoid().segmentalDistanceTo(writable.get()) < cluster.getMedoid().getDelta()) {
                    context.write(new Text(cluster.getMedoid().getLabel()), writable);
                } else {
                    //context.write(new Text("OUTLIER-"+cluster.getLabel()), writable);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
