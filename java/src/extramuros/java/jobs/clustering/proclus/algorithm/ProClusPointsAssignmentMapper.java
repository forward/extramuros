package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 15:48
 */
public class ProClusPointsAssignmentMapper extends Mapper<Writable,VectorWritable,Cluster,VectorWritable> {

    protected ClusterSet clusterSet;

    private static final Logger log = LoggerFactory.getLogger(ProClusPointsAssignmentJob.class);

    protected void map(Writable key, VectorWritable vector, Context context)
      throws IOException, InterruptedException {
        Cluster assignedCluster = null;
        double minDistance = 0;
        boolean  initialized = false;

        for(Cluster cluster : clusterSet.getClusters()) {
            //log.info(" - DISTANCE TO MEDOID: "+cluster.getLabel());
            double dst = cluster.getMedoid().segmentalDistanceTo(vector.get());
            //log.info("     DISTACNE:"+dst+ " VS " + minDistance + "("+initialized+")");
            if(initialized == false || dst < minDistance){
                minDistance = dst;
                assignedCluster = cluster;
                initialized = true;
            }
        }

        //log.info("*** ASSIGNING:"+assignedCluster.getLabel()+" TO VECTOR "+vector.get());
        context.write(assignedCluster,vector);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        Path clusterSetPath = new Path(config.get(ProClusConfigKeys.SET_PATH));
        try {
            clusterSet = readClusterSet(clusterSetPath,config);
        } catch (Exception e) {
            log.error("Error reading cluster set in mapper ",e);
        }
    }

    private ClusterSet readClusterSet(Path input, Configuration config) throws IOException, IllegalAccessException, InstantiationException {

        FileSystem fs = FileSystem.get(config);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,input,config);
        ClusterSet set = new ClusterSet();

        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            reader.next(key,set);
        } finally {
            IOUtils.quietClose(reader);
        }

        log.info("Read initial cluster set:"+set);
        return set;
    }

}
