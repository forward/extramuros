package extramuros.java.jobs.timeseries.aggregate;

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
        HashMap<String,Object> aggregatedValues = new HashMap<String, Object>();

        for(String columnName : table.getHeader().getColumnNames()) {
            aggregatedValues.put(columnName,null);
        }

        int counter = 0;
        Row firstRow = null;
        for(Writable writable : values) {
            Row row = parseValue(writable);
            if(firstRow == null) {
                firstRow = row;
                firstRow.setSchema(table.getHeader().getColumnNames(), table.getHeader().getColumnTypes());
            }

            for(String column : table.getHeader().getColumnNames()) {
                aggregateValue(aggregatedValues, column, row, aggregationFunction);
            }

            counter++;
        }

        if(aggregationFunction.compareTo("average")==0) {
            averageValues(aggregatedValues,counter);
        }

        // creation of the new row
        if(firstRow != null){
            for(String column : table.getHeader().getColumnNames()) {
                firstRow.setValue(column,aggregatedValues.get(column));
            }

            // set the new Date
            Date newDate = new Date(date.get());
            GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime(newDate);
            String dateFormat = calendar.get(Calendar.YEAR)+"-"+calendar.get(Calendar.MONTH)+"-"+calendar.get(Calendar.DAY_OF_MONTH);
            dateFormat = dateFormat + " " + calendar.get(Calendar.HOUR_OF_DAY) + ":" + calendar.get(Calendar.MINUTE);
            dateFormat = dateFormat + ":" + calendar.get(Calendar.SECOND);
            firstRow.setValue(columnName, dateFormat);

            try {
                log.info("AGGREGATING "+counter+" VALUES FOR KEY "+date.get()+" -> " +(new Date(date.get())));
                context.write(date,firstRow);
            } catch (Exception e) {
                log.error("Error writing new row.",e);
            }
        }
    }

    private void averageValues(HashMap<String, Object> aggregatedValues, int counter) {
        for(String columnName : aggregatedValues.keySet()) {
            int position = table.getHeader().positionFor(columnName);
            int type = table.getHeader().getColumnTypes().get(position);

            if(type == RowTypes.DOUBLE) {
                double tmp = (Double) aggregatedValues.get(columnName);
                aggregatedValues.put(columnName, new Double(tmp/counter));
            } else if(type == RowTypes.FLOAT) {
                float tmp = (Float) aggregatedValues.get(columnName);
                aggregatedValues.put(columnName, new Float(tmp/counter));
            } else if(type == RowTypes.INTEGER) {
                int tmp = (Integer) aggregatedValues.get(columnName);
                aggregatedValues.put(columnName, new Integer(tmp/counter));
            } else if(type == RowTypes.LONG) {
                long tmp = (Long) aggregatedValues.get(columnName);
                aggregatedValues.put(columnName, new Long(tmp/counter));
            }
        }
    }

    private void aggregateValue(HashMap<String, Object> aggregatedValues, String columnName, Row row, String aggregationFunction) {
        Object currentValue = aggregatedValues.get(columnName);
        int position = table.getHeader().positionFor(columnName);
        Object newValue = row.getValues().get(position);
        if(currentValue == null){
            aggregatedValues.put(columnName, row.getValues().get(position));
        } else {
            int type = table.getHeader().getColumnTypes().get(position);
            if(type == RowTypes.DOUBLE || type == RowTypes.FLOAT || type == RowTypes.INTEGER || type == RowTypes.LONG) {
                if(aggregationFunction.compareTo("average")==0 || aggregationFunction.compareTo("sum")==0) {
                    if(type == RowTypes.DOUBLE) {
                        double tmp = ((Double) newValue).doubleValue();
                        double tmpCurrent = ((Double)currentValue).doubleValue();
                        aggregatedValues.put(columnName,tmp+tmpCurrent);
                    } else if(type == RowTypes.FLOAT) {
                        float tmp = ((Float) newValue).floatValue();
                        float tmpCurrent = ((Float)currentValue).floatValue();
                        aggregatedValues.put(columnName,tmp+tmpCurrent);
                    } else if(type == RowTypes.INTEGER) {
                        int tmp = ((Integer) newValue).intValue();
                        int tmpCurrent = ((Integer)currentValue).intValue();
                        aggregatedValues.put(columnName,tmp+tmpCurrent);
                    } else if(type == RowTypes.LONG) {
                        long tmp = ((Long) newValue).longValue();
                        long tmpCurrent = ((Long)currentValue).longValue();
                        aggregatedValues.put(columnName,tmp+tmpCurrent);
                    }
                } else if(aggregationFunction.compareTo("max")==0) {
                    if(type == RowTypes.DOUBLE) {
                        double tmp = ((Double) newValue).doubleValue();
                        double tmpCurrent = ((Double)currentValue).doubleValue();
                        if(tmp>tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    } else if(type == RowTypes.FLOAT) {
                        float tmp = ((Float) newValue).floatValue();
                        float tmpCurrent = ((Float)currentValue).floatValue();
                        if(tmp>tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    } else if(type == RowTypes.INTEGER) {
                        int tmp = ((Integer) newValue).intValue();
                        int tmpCurrent = ((Integer)currentValue).intValue();
                        if(tmp>tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    } else if(type == RowTypes.LONG) {
                        long tmp = ((Long) newValue).longValue();
                        long tmpCurrent = ((Long)currentValue).longValue();
                        if(tmp>tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    }
                } else if(aggregationFunction.compareTo("min")==0) {
                    if(type == RowTypes.DOUBLE) {
                        double tmp = ((Double) newValue).doubleValue();
                        double tmpCurrent = ((Double)currentValue).doubleValue();
                        if(tmp<tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    } else if(type == RowTypes.FLOAT) {
                        float tmp = ((Float) newValue).floatValue();
                        float tmpCurrent = ((Float)currentValue).floatValue();
                        if(tmp<tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    } else if(type == RowTypes.INTEGER) {
                        int tmp = ((Integer) newValue).intValue();
                        int tmpCurrent = ((Integer)currentValue).intValue();
                        if(tmp<tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    } else if(type == RowTypes.LONG) {
                        long tmp = ((Long) newValue).longValue();
                        long tmpCurrent = ((Long)currentValue).longValue();
                        if(tmp<tmpCurrent) {
                            aggregatedValues.put(columnName,tmp);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
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
