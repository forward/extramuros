package extramuros.java.jobs.stats.centrality;

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
        int counter = 0;
        double avg = 0;
        double min = 0;
        double max = 0;
        boolean initialized = false;

        for(DoubleWritable value : values) {
            counter++;
            double number = value.get();
            avg = avg + number;
            if(!initialized) {
                initialized = true;
                min = number;
                max = number;
            } else {
                if(number < min) {
                    min = number;
                }
                if(number > max) {
                    max = number;
                }
            }
        }

        try {
            DenseVector v = new DenseVector(4);
            v.set(0, avg/(double)counter);
            v.set(1, min);
            v.set(2, max);
            v.set(3, counter);

            context.write(columnName, new VectorWritable(v));

        } catch (IOException e) {
            log.error("Error reducing average column for column "+columnName.toString(),e);
        } catch (InterruptedException e) {
            log.error("Error reducing average column for column "+columnName.toString(),e);
        }
    }
}
