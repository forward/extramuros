package extramuros.java.jobs.stats.dispersion;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.Table;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, Text, DoubleWritable> {

    protected AbstractTable table;
    protected String columnName;
    protected Double average;

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
        Double number = null;

        try {
            if (!row.isNullAt(position)) {

                if (type == RowTypes.DOUBLE) {
                    number = (Double) row.getValues().get(position);
                } else if (type == RowTypes.FLOAT) {
                    number = ((Float) row.getValues().get(position)).doubleValue();
                } else if (type == RowTypes.INTEGER) {
                    number = ((Integer) row.getValues().get(position)).doubleValue();
                } else if (type == RowTypes.LONG) {
                    number = ((Long) row.getValues().get(position)).doubleValue();
                }

            }
            if (number != null) {
                context.write(new Text(columnName), new DoubleWritable(Math.pow(average - number, 2)));
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


        // average
        average = Double.parseDouble(config.get(JobKeys.AVERAGE));
        log.info("Setup average value: " + average);

        // table info
        Path tablePath = new Path(config.get(JobKeys.PATH));
        log.info("Reading mapper from " + tablePath);
        try {
            table = TableUtils.readAbstractTable(tablePath, config);
        } catch (Exception e) {
            log.error("Error reading Table in mapper ", e);
        }
    }
}
