package extramuros.java.jobs.stats.normalization;

import edu.emory.mathcs.csparsej.tfloat.Scs_cumsum;
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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 15:59
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Writable, Writable, LongWritable, VectorWritable> {

    protected AbstractTable table;
    protected String[] columns;
    protected double[] minValues;
    protected double[] maxValues;

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

        DenseVector vector = new DenseVector(columns.length);

        int i =0;
        boolean success = true;
        for (String columnName : columns) {
            int position = table.getHeader().positionFor(columnName);
            int type = table.getHeader().typeFor(columnName);

            Double number;
            try {
                if(row.isNullAt(position)) {
                    success = false;
                    break;
                }
                if (type == RowTypes.DOUBLE) {
                    number = ((Double) row.getValues().get(position));
                } else if (type == RowTypes.FLOAT) {
                    number = ((Float) row.getValues().get(position)).doubleValue();
                } else if (type == RowTypes.INTEGER) {
                    number = ((Integer) row.getValues().get(position)).doubleValue();
                } else if (type == RowTypes.LONG) {
                    number = ((Long) row.getValues().get(position)).doubleValue();
                } else {
                    success = false;
                    break;
                }

                log.info("NUMBER: "+number);
                log.info("MIN VALUES:"+minValues[i]);
                log.info("MAX VALUES:"+maxValues[i]);
                vector.set(i, ((number - minValues[i]) / (maxValues[i] - minValues[i])));
            } catch (Exception e) {
                log.error("Error retrieving value from row", e);
            }
            i++;
        }

        if(success) {
            Integer id = TableUtils.parseWritableKeyId(key);
            if(id == null) {
                id = TableUtils.randomId();
            }
            context.write(new LongWritable(id), new VectorWritable(vector));
        }
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();

        // column name
        String columnsString = config.get(JobKeys.COLUMNS);
        columns = columnsString.split(",");

        // min values
        String minValuesString = config.get(JobKeys.MIN_VALUES);
        minValues = new double[columns.length];
        int i = 0;
        for (String v : minValuesString.split(",")) {
            minValues[i] = Double.parseDouble(v);
            i++;
        }

        // max values
        String maxValuesString = config.get(JobKeys.MAX_VALUES);
        maxValues = new double[columns.length];
        i = 0;
        for (String v : maxValuesString.split(",")) {
            maxValues[i] = Double.parseDouble(v);
            i++;
        }
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
