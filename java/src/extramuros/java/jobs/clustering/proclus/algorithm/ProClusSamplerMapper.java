package extramuros.java.jobs.clustering.proclus.algorithm;


import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.jet.random.Uniform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class ProClusSamplerMapper extends org.apache.hadoop.mapreduce.Mapper<Writable,VectorWritable,IntWritable,VectorWritable> {

    protected Double samplingProbability;
    protected AbstractTable table;

    //private static final Logger log = LoggerFactory.getLogger(ProClusSamplerMapper.class);

    protected void map(Writable key, VectorWritable value, Context context) throws IOException, InterruptedException {

        int counter = 0;
        Uniform uniform = new Uniform(0,1,(int)new Date().getTime());
        if(uniform.nextDoubleFromTo(0, 1) < samplingProbability) {
            context.write(new IntWritable(counter),value);
            counter++;
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();


        // probability
        samplingProbability = Double.parseDouble(config.get(ProClusConfigKeys.PROBABILITY));

    }
}
