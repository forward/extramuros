package extramuros.java.jobs.timeseries.sort;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, LongWritable, Writable> {

    protected AbstractTable table;
    protected String columnName;

    private static final Logger log = LoggerFactory.getLogger(Mapper.class);

    protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
        Row row = null;
        if (table.isAdapter()) {
            // adapt input
            row = ((AbstractTableAdapter<Writable, Writable>) table).map(key, value);

        } else {
            // force conversion
            row = (Row) value;
        }

        int position = table.getHeader().positionFor(columnName);
        int type = table.getHeader().typeFor(columnName);

        try {
            if (!row.isNullAt(position)) {
                try {
                    Long tmp  = new Long(TableUtils.parseDateAtColumn(row, position, table).getTime());
                    LongWritable number = new LongWritable(tmp);
                    context.write(number, value);
                } catch (Exception ex) {
                    // format exception
                    log.error("Error writing formating date and writing in output.",ex);
                }
            }
        } catch (Exception e) {
            log.error("Error retrieving value from row", e);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

        // column name
        columnName = config.get(JobKeys.COLUMN_NAME);
        log.info("Setup column name: " + columnName);

        // table info
        Path tablePath = new Path(config.get(JobKeys.PATH));
        log.info("Reading mapper from " + tablePath);
        try {
            table = TableUtils.readAbstractTable(tablePath, config);
        } catch (Exception e) {
            log.error("Error reading extramuros.java.visualization.Table in mapper ", e);
        }
    }
}
