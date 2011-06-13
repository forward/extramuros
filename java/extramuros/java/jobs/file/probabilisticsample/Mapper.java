package extramuros.java.jobs.file.probabilisticsample;


import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mahout.math.jet.random.Uniform;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable,Writable,LongWritable,Row> {

    protected Double samplingProbability;
    protected AbstractTable table;
    protected String hash;

    private static final Logger log = LoggerFactory.getLogger(Mapper.class);

    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {

        Row row = null;
        if(table.isAdapter()){
            // adapt input
            row = ((AbstractTableAdapter<Writable, Writable>) table).map(key, value);

        } else {
            // force conversion
            row = (Row) value;
        }

        Uniform uniform = new Uniform(0,1,(int)new Date().getTime());
        if(uniform.nextDoubleFromTo(0, 1) < samplingProbability) {
            log.info("WRITING OUTPUT KEY:" + new IntWritable(row.getId()));
            log.info("WRITING OUTPUT VALUE: " + row);
            context.write(new LongWritable(row.getId()),row);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

        // hash
        hash = UUID.randomUUID().toString();

        // probability
        samplingProbability = Double.parseDouble(config.get(JobKeys.PROBABILITY));

        // table info
        Path tablePath = new Path(config.get(JobKeys.PATH));
        log.info("Reading mapper from "+tablePath);
        try {
            table = TableUtils.readAbstractTable(tablePath, config);
        } catch (Exception e) {
            log.error("Error reading Table in mapper ",e);
        }
    }
}
