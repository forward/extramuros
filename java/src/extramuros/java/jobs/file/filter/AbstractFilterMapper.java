package extramuros.java.jobs.file.filter;


import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public abstract class AbstractFilterMapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, IntWritable, Writable> {

    protected AbstractTable table;
    protected String[] columns;
    protected Class<? extends Vector> vectorClass;
    protected Object filterInformation;

    private static final Logger log = LoggerFactory.getLogger(AbstractFilterMapper.class);

    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {

        Row row = null;
        if (table.isAdapter()) {
            // adapt input
            row = ((AbstractTableAdapter<Writable, Writable>) table).map(key, value);

        } else {
            // force conversion
            row = (Row) value;
        }

        try {

            boolean filterResult = filter(row);

            if(filterResult) {
                context.write(new IntWritable(row.getId()), value);
            }

        } catch (Exception e) {
            for(StackTraceElement trace : e.getStackTrace()) {
                log.error(trace.toString());
            }
            log.error("Error parsing row", e);
        }
    }

    protected abstract boolean filter(Row row);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

        // table info
        Path tablePath = new Path(config.get(JobKeys.PATH));
        log.info("Reading mapper from " + tablePath);
        try {
            table = TableUtils.readAbstractTable(tablePath, config);
        } catch (Exception e) {
            log.error("Error reading extramuros.java.visualization.Table in mapper ", e);
        }

        // filter information

        filterInformation = config.get(JobKeys.FILTER_INFORMATION);

        // custom setup
        customSetup(context);
    }

    protected abstract void customSetup(Context context) throws IOException, InterruptedException;
}
