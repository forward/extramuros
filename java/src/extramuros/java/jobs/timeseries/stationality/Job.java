package extramuros.java.jobs.timeseries.stationality;

import extramuros.java.formats.*;
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
import java.util.ArrayList;
import java.util.HashMap;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:31
 */

/**
 * Computes the average and variance for a time series and period so the stationality can be checked.
 */
public class Job extends ExtramurosJob {

    protected String columnName;
    protected AbstractTable table;
    protected Path outputPath;
    protected String period;
    protected String dateColumn;


    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String columnName, String dateColumn, String period, AbstractTable table, String outputPath, Configuration configuration) throws IOException {
        super(configuration);

        this.columnName = columnName;
        this.table = table;
        this.outputPath = new Path(outputPath);
        this.period = period;
        this.dateColumn = dateColumn;

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
        ArrayList<String> columnNames = new ArrayList<String>();
        columnNames.add("date");
        columnNames.add("average");
        columnNames.add("variance");
        columnNames.add("minimum");
        columnNames.add("maximum");

        ArrayList<Integer> columnTypes = new ArrayList<Integer>();
        columnTypes.add(RowTypes.DATE_TIME);
        columnTypes.add(RowTypes.DOUBLE);
        columnTypes.add(RowTypes.DOUBLE);
        columnTypes.add(RowTypes.DOUBLE);
        columnTypes.add(RowTypes.DOUBLE);


        TableHeader header = new TableHeader(columnNames,columnTypes);

        Table outputTable = new Table(header, outputPath.toUri().getPath());
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
            getConf().set(JobKeys.DATE_COLUMN_NAME, dateColumn);
            getConf().set(JobKeys.PERIOD, period);


            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "time series stationality job for column: " + columnName));

            getJob().setJobName("time_series_stationality");

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
