package extramuros.java.jobs.stats.dispersion;

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
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text,DoubleWritable,Text,VectorWritable>{

    private static final Logger log = LoggerFactory.getLogger(Reducer.class);

    @Override
    protected void reduce(Text columnName, Iterable<DoubleWritable> values, Context context) {
        double counter = 0;
        double sum = 0;


        for(DoubleWritable value : values) {
            counter++;
            double number = value.get();
            sum = sum + number;
        }

        try {
            DenseVector v = new DenseVector(2);
            v.set(0,sum/counter);
            v.set(1, Math.sqrt(sum/counter));

            context.write(columnName, new VectorWritable(v));

        } catch (IOException e) {
            log.error("Error reducing dispersion stats for column "+columnName.toString(),e);
        } catch (InterruptedException e) {
            log.error("Error reducing dispersion stats for column "+columnName.toString(),e);
        }
    }
}
