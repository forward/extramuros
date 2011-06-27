package extramuros.java.jobs.file.vectorize;


import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.mahout.math.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.UUID;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, IntWritable, VectorWritable> {

    protected AbstractTable table;
    protected String[] columns;
    protected Class<? extends Vector> vectorClass;

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

        try {
            // Retrieving the values from the row

            double[] values = new double[columns.length];
            int counter = 0;
            for (String column : columns) {
                int position = table.getHeader().positionFor(column);
                int type = table.getHeader().typeFor(column);
                Double val = null;

                if(row.isNullAt(position)) {
                    throw (new Exception("Null value parsing row at column " + column));
                }

                if (type == RowTypes.DOUBLE) {
                    val = (Double) row.getValues().get(position);
                } else if (type == RowTypes.FLOAT) {
                    val = ((Float) row.getValues().get(position)).doubleValue();
                } else if (type == RowTypes.INTEGER) {
                    val = ((Integer) row.getValues().get(position)).doubleValue();
                } else if (type == RowTypes.LONG) {
                    val = ((Long) row.getValues().get(position)).doubleValue();
                }

                if (val != null) {
                    values[counter] = val;
                } else {
                    throw (new Exception("Null value parsing row at column " + column));
                }
                counter++;
            }

            // Building the right vector using reflection

            Class[] constructorArgTypes = new Class[1];
            constructorArgTypes[0] = Integer.TYPE;

            Constructor ctr = vectorClass.getConstructor(constructorArgTypes);
            Object[] constructorArgs = new Object[1];
            constructorArgs[0]  = new Integer(columns.length);

            Vector v = (Vector) ctr.newInstance(constructorArgs);

            // Setting the values in the vector

            for(int i=0; i<values.length; i++){
                double val = values[i];
                if(vectorClass == RandomAccessSparseVector.class || vectorClass == SequentialAccessSparseVector.class) {
                    if(val != 0) {
                        v.set(i,val);
                    }
                } else {
                    v.set(i,val);
                }
            }


            Integer id = TableUtils.parseWritableKeyId(key);
            if(id == null) {
                id = TableUtils.randomId();
            }


            context.write(new IntWritable(id),new VectorWritable(v));

        } catch (Exception e) {
            for(StackTraceElement trace : e.getStackTrace()) {
                log.error(trace.toString());
            }
            log.error("Error parsing row", e);
        }
    }

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

        // vector class
        try {
            vectorClass = Class.forName(config.get(JobKeys.CLASS_NAME)).asSubclass(Vector.class);
        } catch (ClassNotFoundException e) {
            log.error("Error reading vector class", e);
        }

        // columns
        String columnsString = config.get(JobKeys.COLUMNS);
        columns = columnsString.split(",");
    }
}
