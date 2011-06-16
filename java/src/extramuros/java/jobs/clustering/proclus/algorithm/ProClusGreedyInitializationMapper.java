package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 24/05/2011
 * Time: 12:35
 */
public class ProClusGreedyInitializationMapper extends Mapper<Writable,VectorWritable,MedoidSet,WeightedVectorWritable> {

    private MedoidSet medoids;

    private static final Logger log = LoggerFactory.getLogger(ProClusGreedyInitializationMapper.class);

    protected void map(Writable key, VectorWritable vector, Context context)
      throws IOException, InterruptedException {


        double dist = medoids.minDist(vector.get());

        WeightedVectorWritable weightedVector = new WeightedVectorWritable(dist,vector.get());

        context.write(medoids,weightedVector);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        Path medoidsSetPath = new Path(config.get(ProClusConfigKeys.SET_PATH));
        log.info(" ** MAPPER CONFIG");
        log.info(config.toString());
        FileSystem fs = FileSystem.get(config);
        log.info(" ** EXITS? "+fs.exists(medoidsSetPath));
        try {
            //medoids = readMedoidsSet(medoidsSetPath, config);
            log.info("CHANGING "+medoidsSetPath.toUri().getPath() + " BY " + (new Path("/user/antonio/data/20110607/es/proclus/greedy/medoids-0/part-r-00000")).toUri().getPath());
            //medoidsSetPath = new Path("/user/antonio/data/20110607/es/proclus/greedy/medoids-0/part-r-00000");
            SequenceFile.Reader reader = new SequenceFile.Reader(fs,medoidsSetPath,config);
            medoids = new MedoidSet();

            Writable key = (Writable) reader.getKeyClass().newInstance();
            reader.next(key,medoids);

        } catch (Exception e) {
            log.info(" ** REALLY EXITS? "+fs.exists(medoidsSetPath));
            log.error("Error reading medoid set in mapper ",e);
        }
    }
}
