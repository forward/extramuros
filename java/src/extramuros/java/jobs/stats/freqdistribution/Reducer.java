package extramuros.java.jobs.stats.freqdistribution;

import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:25
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable>{

    private static final Logger log = LoggerFactory.getLogger(Reducer.class);

    @Override
    protected void reduce(DoubleWritable valueFreq, Iterable<DoubleWritable> values, Context context) {
        double sum = 0;


        for(DoubleWritable value : values) {
            double number = value.get();
            sum = sum + number;
        }

        try {
            context.write(valueFreq, new DoubleWritable(sum));
        } catch (IOException e) {
            log.error("Error reducing frequency distribution stats ",e);
        } catch (InterruptedException e) {
            log.error("Error reducing frequency distribution stats ",e);
        }
    }
}
