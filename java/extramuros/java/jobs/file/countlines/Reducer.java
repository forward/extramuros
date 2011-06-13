package extramuros.java.jobs.file.countlines;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:25
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>{

    private static final Logger log = LoggerFactory.getLogger(Reducer.class);

    @Override
    protected void reduce(Text columnName, Iterable<IntWritable> values, Context context) {
        int counter = 0;

        for(IntWritable value : values) {
            counter++;
        }

        try {
             context.write(columnName, new IntWritable(counter));

        } catch (IOException e) {
            log.error("Error reducing dispersion stats for column "+columnName.toString(),e);
        } catch (InterruptedException e) {
            log.error("Error reducing dispersion stats for column "+columnName.toString(),e);
        }
    }
}
