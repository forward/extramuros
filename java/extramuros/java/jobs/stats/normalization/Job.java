package extramuros.java.jobs.stats.normalization;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.TableHeader;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.formats.adapters.VectorSeqTableAdapter;
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
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:31
 */

/**
 * The stats.normalization Job normalize the value of a table for certain
 * columns writing the normalized and vectorized output in a new table.
 */
public class Job extends ExtramurosJob {

    protected AbstractTable table;
    protected Path outputPath;
    protected String minValues;
    protected String maxValues;
    protected String columns;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String[] columns, double[] minValues, double[] maxValues, AbstractTable table, String outputPath, Configuration configuration) throws IOException {
        super(configuration);

        StringBuilder columnsSB = new StringBuilder(columns[0]);
        StringBuilder minValuesSB = new StringBuilder("" + minValues[0]);
        StringBuilder maxValuesSB = new StringBuilder("" + maxValues[0]);
        for (int i = 1; i < columns.length; i++) {
            columnsSB.append("," + columns[i]);
            minValuesSB.append("," + minValues[i]);
            maxValuesSB.append("," + maxValues[i]);
        }

        this.minValues = minValuesSB.toString();
        this.maxValues = maxValuesSB.toString();
        this.columns = columnsSB.toString();
        this.table = table;
        this.outputPath = new Path(outputPath);
    }

    public Path getOutputFile() {
        return outputPath;
    }

    public Object getOutput() {
        String[] columnsArray = columns.split(",");
        ArrayList<String> newColumnNames = new ArrayList<String>(columnsArray.length);
        ArrayList<Integer> newColumnTypes = new ArrayList<Integer>(columnsArray.length);

        for(String columnName : columnsArray) {
            newColumnNames.add(columnName);
            newColumnTypes.add(RowTypes.DOUBLE);
        }

        TableHeader header = new TableHeader(newColumnNames, newColumnTypes);
        VectorSeqTableAdapter table = new VectorSeqTableAdapter(header,outputPath.toUri().getPath());

        table.setConfiguration(getConf());
        table.setTablePath(outputPath.suffix(".tbl").toUri().getPath());

        return table;
    }

    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
            getConf().set(JobKeys.COLUMNS, columns);
            getConf().set(JobKeys.MAX_VALUES, maxValues);
            getConf().set(JobKeys.MIN_VALUES, minValues);


            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "normalization job"));

            getJob().setJobName("centrality_stats_job");

            getJob().setMapperClass(Mapper.class);

            getJob().setMapOutputKeyClass(LongWritable.class);
            getJob().setMapOutputValueClass(VectorWritable.class);

            getJob().setOutputKeyClass(LongWritable.class);
            getJob().setOutputValueClass(VectorWritable.class);

            getJob().setNumReduceTasks(0);

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
                throw new InterruptedException("Stats algorithm failed processing " + new Path(table.getRowsPath()).toUri().getPath()
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
