package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 24/05/2011
 * Time: 12:47
 */
public class ProClusGreedyInitializationReducer extends Reducer<MedoidSet,WeightedVectorWritable,IntWritable,MedoidSet> {

    private static final Logger log = LoggerFactory.getLogger(ProClusGreedyInitializationReducer.class);

    private int interationNum;

    @Override
    protected void reduce(MedoidSet key, Iterable<WeightedVectorWritable> values, Context context)
      throws IOException, InterruptedException {
        double maxDist = 0;
        MedoidSet set = null;
        boolean initialized = false;
        Vector nextMedoidVector = null;

        for (WeightedVectorWritable vector : values) {
            double distance = vector.getWeight();

            if(initialized == false) {
                maxDist = distance;
                nextMedoidVector = vector.getVector();
                set = key;
                initialized = true;
            } else {
                double tmpDist = vector.getWeight();

                if(tmpDist > maxDist) {
                    maxDist = tmpDist;
                    nextMedoidVector = vector.getVector();
                }
            }
        }


        if(nextMedoidVector != null) {
            set.addMedoid(new Medoid(nextMedoidVector));
            log.info("ABOUT TO WRITE IN REDUCER: "+set.count() + " , " + set);
            log.info("MAX DIST:"+maxDist);
            for(int i=0; i<set.getMedoids().length; i++) {
                Medoid current = set.getMedoids()[i];
                log.info(i+")"+ current.getVector());
            }

            // compute the deltas for this medoid set
            set.computeDeltas();

            context.write(new IntWritable(set.count()), set);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();

        interationNum = Integer.parseInt(conf.get(ProClusConfigKeys.ITERATION_NUM_KEY));
    }


}
