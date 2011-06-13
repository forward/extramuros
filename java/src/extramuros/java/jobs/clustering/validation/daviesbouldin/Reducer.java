package extramuros.java.jobs.clustering.validation.daviesbouldin;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:25
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text,DoubleWritable,Text,DoubleWritable>{

    private static final Logger log = LoggerFactory.getLogger(Reducer.class);

    @Override
    protected void reduce(Text clusterId, Iterable<DoubleWritable> values, Context context) {
        double counter = 0;
        double sum = 0;


        for(DoubleWritable value : values) {
            counter++;
            double number = value.get();
            sum = sum + number;
        }

        try {
            context.write(clusterId, new DoubleWritable(Math.sqrt(sum/counter)));

        } catch (IOException e) {
            log.error("Error reducing Davies-Bouldin reducer for cluster "+clusterId.toString(),e);
        } catch (InterruptedException e) {
            log.error("Error reducing Davies-Bouldin reducer for cluster "+clusterId.toString(),e);
        }
    }
}
