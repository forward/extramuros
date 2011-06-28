package extramuros.java.jobs.timeseries.stationality;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:25
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<LongWritable,Writable,LongWritable,Row>{

    private static final Logger log = LoggerFactory.getLogger(Reducer.class);
    private String columnName;
    private AbstractTable table;
    private String aggregationFunction;

    @Override
    protected void reduce(LongWritable date, Iterable<Writable> values, Context context) {
        Double average = null;
        Double min = null;
        Double max = null;
        ArrayList<Double> coll = new ArrayList<Double>();

        int counter = 0;
        Row firstRow = null;
        for(Writable writable : values) {
            Row row = parseValue(writable);
            if(average == null) {

            }

            aggregateValue(average, min, max, columnName, coll, row, aggregationFunction);

            counter++;
        }



        // creation of the new row
        if(firstRow != null){
            average = ((Double)average) / ((double) (counter-1));

            double variance = 0;
            for(Double point : coll) {
                variance = variance + Math.pow((point - ((Double) average)),2);
            }

            variance = variance / (double ) counter;

            // set the new Date
            Date newDate = new Date(date.get());
            GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime(newDate);
            String dateFormat = calendar.get(Calendar.YEAR)+"-"+calendar.get(Calendar.MONTH)+"-"+calendar.get(Calendar.DAY_OF_MONTH);
            dateFormat = dateFormat + " " + calendar.get(Calendar.HOUR_OF_DAY) + ":" + calendar.get(Calendar.MINUTE);
            dateFormat = dateFormat + ":" + calendar.get(Calendar.SECOND);


            ArrayList<Object> rowValues = new ArrayList<Object>();
            rowValues.add(dateFormat);
            rowValues.add(average);
            rowValues.add(variance);
            rowValues.add(min);
            rowValues.add(max);

            Row row = new Row(TableUtils.randomId(),rowValues);

            try {
                log.info("AGGREGATING "+counter+" VALUES FOR KEY "+date.get()+" -> " +(new Date(date.get())));
                context.write(date,row);
            } catch (Exception e) {
                log.error("Error writing new row.",e);
            }
        }
    }

    private void aggregateValue(Double avg, Double min, Double max, String columnName, ArrayList<Double> coll, Row row, String aggregationFunction) {
        int position = table.getHeader().positionFor(columnName);
        Object newValue = row.getValues().get(position);
        int type = table.getHeader().getColumnTypes().get(position);


        Double tmp = null;

        if(type == RowTypes.DOUBLE) {
            tmp = ((Double) newValue);
        } else if(type == RowTypes.FLOAT) {
            tmp = new Double(((Float) newValue).doubleValue());
        } else if(type == RowTypes.INTEGER) {
            tmp = new Double(((Integer) newValue).doubleValue());
        } else if(type == RowTypes.LONG) {
            tmp = new Double(((Long) newValue).doubleValue());
        }

        if(tmp != null){
            avg = new Double(avg + tmp);
            if(tmp < min) {
                min = tmp;
            }
            if(tmp > max) {
                max = tmp;
            }

            coll.add(tmp);
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

        // aggraegation function
        aggregationFunction = config.get(JobKeys.FILTER_INFORMATION);
        log.info("Aggregation function: "+aggregationFunction);
    }

    protected Row parseValue(Writable value) {
        Row row = null;
        if (table.isAdapter()) {
            // adapt input
            row = ((AbstractTableAdapter<Writable, Writable>) table).map(new Text("tmp"), value);

        } else {
            // force conversion
            row = (Row) value;
        }

        return row;
    }
}
