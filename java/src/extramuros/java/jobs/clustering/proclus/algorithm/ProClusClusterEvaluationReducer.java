package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 16:17
 */
public class ProClusClusterEvaluationReducer extends Reducer<Cluster,VectorWritable,Cluster,DoubleWritable> {

    private static final Logger log = LoggerFactory.getLogger(ProClusClusterEvaluationReducer.class);

    @Override
    protected void reduce(Cluster cluster, Iterable<VectorWritable> values, Context context) {

        try {
            Medoid medoid = cluster.getMedoid();
            int counter = 0;
            double[] acum = null;


            for(VectorWritable writable : values) {
                counter++;
                if(acum == null){
                    acum = medoid.segmentalDimensionDistances(writable.get());
                } else {
                    double[] newDimensions = medoid.segmentalDimensionDistances(writable.get());
                    for(int i=0; i<acum.length; i++) {
                        acum[i] = acum[i] + newDimensions[i];
                    }
                }
            }

            double wi = 0;
            for(int i=0; i<acum.length; i++){
                wi = wi + (acum[i] / counter);
            }

            wi = wi / acum.length;



            log.info("*** WRITING EVALUATION METRIC FOR : "+cluster.getLabel());
            log.info("W_i: "+wi);

            context.write(cluster, new DoubleWritable(wi));
        } catch (Exception e) {
            log.error("Error in ClusterEvaluationReducer.",e);
        }
    }
}
