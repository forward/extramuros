package extramuros.java.jobs.stats.freqdistribution;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * Date: 02/06/2011
 * Time: 09:57
 */
public class Job extends ExtramurosJob {

    protected String columnName;
    protected AbstractTable table;
    protected Path outputPath;

    private static final Logger log = LoggerFactory.getLogger(Job.class);

    public Job(String columnName, AbstractTable table, String outputPath, Configuration configuration) throws IOException {
        super(configuration);

        this.columnName = columnName;
        this.table = table;
        this.outputPath = new Path(outputPath);
    }

    @Override
    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
            getConf().set(JobKeys.COLUMN_NAME, columnName);


            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "stats job for column: " + columnName));

            getJob().setJobName("frequency_distribution_job");

            getJob().setMapperClass(Mapper.class);
            getJob().setReducerClass(Reducer.class);

            getJob().setMapOutputKeyClass(DoubleWritable.class);
            getJob().setMapOutputValueClass(DoubleWritable.class);

            getJob().setOutputKeyClass(DoubleWritable.class);
            getJob().setOutputValueClass(DoubleWritable.class);

            if(table.isAdapter()) {
                getJob().setInputFormatClass(((AbstractTableAdapter<Writable,Writable>)table).inputFormat());
            } else {
                getJob().setInputFormatClass(SequenceFileInputFormat.class);
            }
            getJob().setOutputFormatClass(SequenceFileOutputFormat.class);

            String inputPathString = composeInputPathString(new Path(table.getRowsPath()));
            FileInputFormat.addInputPaths(getJob(),inputPathString);
            FileOutputFormat.setOutputPath(getJob(), outputPath);

            getJob().setJarByClass(Job.class);

            if (!getJob().waitForCompletion(true)) {
                throw new InterruptedException("Frequency distribution stats failed processing " + new Path(table.getRowsPath()).toUri().getPath()
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
        try {
            extramuros.java.jobs.file.combine.Job combiner = new extramuros.java.jobs.file.combine.Job(outputPath,
                    outputPath.suffix("/out.seq"),
                    getConf());
            combiner.run();
            return combiner.getOutputFile();
        } catch (IOException e) {
            log.error("Error combining outputs");
            return null;
        }
    }

    @Override
    public Object getOutput() {
        try {
            return TableUtils.fileSeqIterator(getOutputFile(),getConf());
        } catch (IOException e) {
            log.error("Error creating iterator for the output job file",e);
            return null;
        }
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
