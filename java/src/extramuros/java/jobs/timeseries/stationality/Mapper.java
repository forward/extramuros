package extramuros.java.jobs.timeseries.stationality;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, LongWritable, Writable> {

    protected AbstractTable table;
    protected String columnName;
    protected String period;

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
                    Date date = new Date(TableUtils.parseDateAtColumn(row, position, table).getTime());

                    if(period.compareTo("year")==0) {
                        GregorianCalendar calendar = new GregorianCalendar();
                        calendar.setTime(date);
                        calendar.set(Calendar.MONTH,0);
                        calendar.set(Calendar.DAY_OF_MONTH, 0);
                        calendar.set(Calendar.HOUR_OF_DAY,0);
                        calendar.set(Calendar.HOUR, 0);
                        calendar.set(Calendar.MINUTE,0);
                        calendar.set(Calendar.SECOND,0);
                        calendar.set(Calendar.MILLISECOND,0);
                        date = calendar.getTime();
                    } else if(period.compareTo("month")==0) {
                        GregorianCalendar calendar = new GregorianCalendar();
                        calendar.setTime(date);
                        calendar.set(Calendar.DAY_OF_MONTH,0);
                        calendar.set(Calendar.HOUR_OF_DAY,0);
                        calendar.set(Calendar.HOUR, 0);
                        calendar.set(Calendar.MINUTE,0);
                        calendar.set(Calendar.SECOND,0);
                        calendar.set(Calendar.MILLISECOND,0);
                        date = calendar.getTime();
                    } else if(period.compareTo("week")==0) {
                        GregorianCalendar calendar = new GregorianCalendar();
                        calendar.setTime(date);
                        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                        calendar.set(Calendar.HOUR_OF_DAY,0);
                        calendar.set(Calendar.MINUTE,0);
                        calendar.set(Calendar.SECOND,0);
                        calendar.set(Calendar.MILLISECOND,0);
                        date = calendar.getTime();
                    } else if(period.compareTo("day")==0) {
                        GregorianCalendar calendar = new GregorianCalendar();
                        calendar.setTime(date);
                        calendar.set(Calendar.HOUR_OF_DAY,0);
                        calendar.set(Calendar.MINUTE,0);
                        calendar.set(Calendar.SECOND, 0);
                        calendar.set(Calendar.MILLISECOND,0);
                        date = calendar.getTime();
                    }



                    Date oldDate = new Date(TableUtils.parseDateAtColumn(row, position, table).getTime());
                    GregorianCalendar cal = new GregorianCalendar();
                    cal.setTime(oldDate);
                    LongWritable number = new LongWritable(date.getTime());
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
        columnName = config.get(JobKeys.DATE_COLUMN_NAME);
        log.info("Setup column name: " + columnName);

        // table info
        Path tablePath = new Path(config.get(JobKeys.PATH));
        log.info("Reading mapper from " + tablePath);
        try {
            table = TableUtils.readAbstractTable(tablePath, config);
        } catch (Exception e) {
            log.error("Error reading extramuros.java.visualization.Table in mapper ", e);
        }

        // period
        period = config.get(JobKeys.PERIOD);
        log.info("Aggregation period: "+period);
    }
}
