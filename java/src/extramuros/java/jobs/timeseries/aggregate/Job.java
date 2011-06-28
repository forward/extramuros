package extramuros.java.jobs.timeseries.aggregate;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.Table;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:31
 */

/**
 * Aggregate the values in a time series using a different time period
 */
public class Job extends ExtramurosJob {

    protected String columnName;
    protected AbstractTable table;
    protected Path outputPath;
    protected String period;
    protected String aggregationFunction;


    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String columnName, String period, String aggregationFunction, AbstractTable table, String outputPath, Configuration configuration) throws IOException {
        super(configuration);

        this.columnName = columnName;
        this.table = table;
        this.outputPath = new Path(outputPath);
        this.period = period;
        this.aggregationFunction = aggregationFunction;

        int counter = 0;
        boolean found = false;
        for (String label : table.getHeader().getColumnNames()) {
            if (label.compareTo(columnName) == 0) {
                if (table.getHeader().getColumnTypes().get(counter) != RowTypes.DATE_TIME) {
                    throw (new IOException("The requested column " + columnName + " is not of type DATE_TIME"));
                } else {
                    found = true;
                }
                break;
            }
            counter++;
        }
        if (found == false) {
            throw (new IOException("The requested column could not be found"));
        }
    }

    public Path getOutputFile() {
        try {
            return outputPath;
        } catch (Exception e) {
            log.error("Error generating output file", e);
            return null;
        }

    }

    public Object getOutput() {

        Table outputTable = new Table(table.getHeader().clone(), outputPath.toUri().getPath());
        outputTable.setConfiguration(getConf());
        outputTable.setTablePath(outputPath.suffix(".tbl").toUri().getPath());

        HashMap<String,String> dateFormats = outputTable.getHeader().getDateFormats();
        dateFormats.put(columnName,"yyyy-MM-dd hh:mm:ss");

        return outputTable;
    }

    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
            getConf().set(JobKeys.COLUMN_NAME, columnName);
            getConf().set(JobKeys.FILTER_INFORMATION, aggregationFunction);
            getConf().set(JobKeys.PERIOD, period);


            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "time series aggregation job for column: " + columnName));

            getJob().setJobName("time_series_aggregate");

            getJob().setMapperClass(Mapper.class);
            getJob().setReducerClass(Reducer.class);

            getJob().setMapOutputKeyClass(LongWritable.class);
            getJob().setMapOutputValueClass(table.getRowClass());

            getJob().setOutputKeyClass(LongWritable.class);
            getJob().setOutputValueClass(Row.class);

            if (table.isAdapter()) {
                getJob().setInputFormatClass(((AbstractTableAdapter<Writable, Writable>) table).inputFormat());
            } else {
                getJob().setInputFormatClass(SequenceFileInputFormat.class);
            }
            getJob().setOutputFormatClass(SequenceFileOutputFormat.class);

            String inputPathString = composeInputPathString(new Path(table.getRowsPath()));
            FileInputFormat.setInputPaths(getJob(), inputPathString);
            FileOutputFormat.setOutputPath(getJob(), outputPath);

            getJob().setJarByClass(Job.class);

            if (!getJob().waitForCompletion(true)) {
                throw new InterruptedException("Aggregate time series algorithm failed processing " + new Path(table.getRowsPath()).toUri().getPath()
                        + " output: " + outputPath.toUri().getPath());
            }

            cleanOutput(outputPath);

        } catch (IOException e) {
            log.error("Error running job", e);
        } catch (InterruptedException e) {
            log.error("Error running job", e);
        } catch (ClassNotFoundException e) {
            log.error("Error running job", e);
        }
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
