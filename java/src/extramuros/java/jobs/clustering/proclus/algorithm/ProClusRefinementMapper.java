package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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
 * Time: 10:33
 */
public class ProClusRefinementMapper extends Mapper<Text,VectorWritable,Medoid,MedoidDimensionalData> {

    private ClusterSet clusterSet;
    private MedoidSet  medoidSet;

    private static final Logger log = LoggerFactory.getLogger(ProClusRefinementMapper.class);

    protected void map(Text key, VectorWritable vector, Context context)
      throws IOException, InterruptedException {

        for(Medoid medoid : medoidSet.getMedoids()) {
            if(medoid.inLocality(vector.get())) {
                try {

                    if(medoid.getLabel().equalsIgnoreCase(key.toString())) {
                        double[] distances = medoid.dimensionDistances(vector.get());

                        MedoidDimensionalData dimensionalData = new MedoidDimensionalData(medoid);
                        dimensionalData.setData(distances);

                        context.write(medoid,dimensionalData);
                    }

                } catch (Exception e) {
                    log.error("Exception computing distances for vector:"+vector.get(),e);
                }
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        Path clusterSetPath = new Path(config.get(ProClusConfigKeys.SET_PATH));
        try {
            clusterSet = readClusterSet(clusterSetPath,config);
            medoidSet = new MedoidSet();
            for(Cluster cluster : clusterSet.getClusters()) {
                medoidSet.addMedoid(cluster.getMedoid());
            }
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
