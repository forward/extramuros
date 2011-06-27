package extramuros.java.jobs.clustering.validation.daviesbouldin;

import extramuros.java.jobs.utils.ClusterUtils;
import extramuros.java.jobs.utils.JobKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, Text, DoubleWritable> {

    protected HashMap<Integer,Cluster> clustersMap;

    private static final Logger log = LoggerFactory.getLogger(Mapper.class);

    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
        try {
            int clusterId = Integer.parseInt(key.toString());
            Vector vector = null;
            Cluster cluster = clustersMap.get(clusterId);
            if(value instanceof VectorWritable) {
                vector = ((VectorWritable) value).get();
            } else if(value instanceof WeightedVectorWritable) {
                vector = ((WeightedVectorWritable) value).getVector();
            }

            if(vector != null && cluster != null) {
                ManhattanDistanceMeasure distance = new ManhattanDistanceMeasure();
                double dist = Math.pow(distance.distance(vector,cluster.getCenter()),2.0);

                Text outputKey = null;
                if(key instanceof Text) {
                    outputKey = (Text) key;
                } else if(key instanceof IntWritable) {
                    outputKey = new Text(""+((IntWritable)key).get());
                }

                if(outputKey != null) {
                    context.write(outputKey, new DoubleWritable(dist));
                }
            }
        } catch (Exception e) {
            log.error("Error retrieving value from row", e);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

        // clusters path
        Path clustersPath = new Path(config.get(JobKeys.PATH));
        log.info("Reading clusters from " + clustersPath);
        clustersMap = new HashMap<Integer, Cluster>();
        try {
            Iterator<Cluster> it = ClusterUtils.clusterIterator(clustersPath,config);
            while(it.hasNext()) {
                Cluster cluster = it.next();
                clustersMap.put(cluster.getId(), cluster);
            }
        } catch (Exception e) {
            log.error("Error reading clusters in mapper ", e);
        }
    }
}
