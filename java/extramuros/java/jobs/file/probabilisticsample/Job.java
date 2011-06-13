package extramuros.java.jobs.file.probabilisticsample;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.Row;
import extramuros.java.formats.Table;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
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
    protected Double samplingProbabilitiy;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String outputPath, Double samplingProbability, AbstractTable table, Configuration configuration) throws IOException {
        super(configuration);
        this.samplingProbabilitiy = samplingProbability;
        this.table = table;
        this.outputPath = new Path(outputPath);
        this.outputFile = this.outputPath.suffix("/rows.rows");
    }

    @Override
    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
            getConf().set(JobKeys.PROBABILITY, samplingProbabilitiy.toString());

            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "probabilistic sample job. "));

            getJob().setJobName("probabilistic_sample_job");

            getJob().setMapperClass(Mapper.class);
            getJob().setNumReduceTasks(0);
            //getJob().setReducerClass(Reducer.class);

            getJob().setMapOutputKeyClass(LongWritable.class);
            getJob().setMapOutputValueClass(Row.class);

            getJob().setOutputKeyClass(LongWritable.class);
            getJob().setOutputValueClass(Row.class);


            if(table.isAdapter()) {
                getJob().setInputFormatClass(((AbstractTableAdapter<Writable,Writable>)table).inputFormat());
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
                throw new InterruptedException("Probabilistic sample job failed processing " + new Path(table.getRowsPath()).toUri().getPath()
                        + " output: " + outputPath.toUri().getPath());
            }

            cleanOutput(outputPath);

            extramuros.java.jobs.file.combine.Job combiner = new extramuros.java.jobs.file.combine.Job(outputPath,outputFile,getConf());

            combiner.run();

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

        Table outputTable = new Table(table.getHeader(),outputFile.toUri().getPath());
        outputTable.setConfiguration(getConf());
        return outputTable;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
