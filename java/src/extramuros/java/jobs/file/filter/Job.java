package extramuros.java.jobs.file.filter;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 16:50
 */
public class Job extends ExtramurosJob {

    protected Path outputPath;
    protected AbstractTable table;
    protected Path outputFile;
    protected Class<? extends AbstractFilterMapper> mapperClass;
    protected String filterInfo;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(Path outputPath, AbstractTable table, Class<? extends AbstractFilterMapper> mapperClass, String filterInfo, Configuration configuration) throws IOException {
        super(configuration);
        this.table = table;
        this.outputPath = outputPath;
        this.outputFile = this.outputPath;
        this.mapperClass = mapperClass;
        this.filterInfo = filterInfo;

    }

    @Override
    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
            getConf().set(JobKeys.FILTER_INFORMATION, (String) filterInfo);

            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "filter table job. "));

            getJob().setJobName("filter_table_job");

            getJob().setMapperClass(mapperClass);
            getJob().setNumReduceTasks(0);

            getJob().setMapOutputKeyClass(IntWritable.class);
            getJob().setMapOutputValueClass(table.getRowClass());
            getJob().setOutputKeyClass(IntWritable.class);
            getJob().setOutputValueClass(table.getRowClass());

            if(table.isAdapter()) {
                getJob().setInputFormatClass(((AbstractTableAdapter<Writable, Writable>) table).inputFormat());
            } else {
                getJob().setInputFormatClass(SequenceFileInputFormat.class);
            }
            getJob().setOutputFormatClass(SequenceFileOutputFormat.class);

            String inputPathString = composeInputPathString(new Path(table.getRowsPath()));
            log.info("Composed input path "+inputPathString);
            FileInputFormat.addInputPaths(getJob(), inputPathString);
            FileOutputFormat.setOutputPath(getJob(), outputPath);

            getJob().setJarByClass(Job.class);

            if (!getJob().waitForCompletion(true)) {
                throw new InterruptedException("Filter job failed processing " + new Path(table.getRowsPath()).toUri().getPath()
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

    @Override
    public Path getOutputFile() {
        return outputFile;
    }

    @Override
    public Object getOutput() {
        AbstractTable outputTable = table.clone();
        outputTable.setConfiguration(getConf());

        outputTable.setRowsPath(outputPath.toUri().getPath());
        outputTable.setTablePath(outputPath.suffix(".tbl").toUri().getPath());

        return outputTable;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
