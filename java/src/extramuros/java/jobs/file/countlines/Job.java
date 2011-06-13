package extramuros.java.jobs.file.countlines;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import extramuros.java.jobs.utils.TableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;

/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 16:50
 */
public class Job extends ExtramurosJob {

    protected Path outputPath;
    protected AbstractTable table;
    protected Path outputFile;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(Path outputPath, AbstractTable table, Configuration configuration) throws IOException {
        super(configuration);
        this.table = table;
        this.outputPath = outputPath;
        this.outputFile = this.outputPath.suffix("/count.seq");

    }

    @Override
    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());

            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "count lines job. "));

            getJob().setJobName("count_lines_job");

            getJob().setMapperClass(Mapper.class);
            getJob().setReducerClass(Reducer.class);

            getJob().setMapOutputKeyClass(Text.class);
            getJob().setMapOutputValueClass(IntWritable.class);

            getJob().setOutputKeyClass(Text.class);
            getJob().setOutputValueClass(IntWritable.class);

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
                throw new InterruptedException("Dispersion stats failed processing " + new Path(table.getRowsPath()).toUri().getPath()
                        + " output: " + outputPath.toUri().getPath());
            }

            cleanOutput(outputPath);

            Path[] outputs = getAllChildrenFiles(outputPath);
            int acum = 0;
            for(Path output : outputs) {
                try {
                    SequenceFile.Reader reader = new SequenceFile.Reader(getFs(),output,getConf());
                    Writable key = (Writable) reader.getKeyClass().newInstance();
                    IntWritable tmp = new IntWritable();

                    while(reader.next(key,tmp)) {
                        acum = acum + tmp.get();
                    }
                } finally {
                    getFs().delete(output,true);
                }
            }

            TableUtils.writeSingleWritable(outputPath.suffix("/count.seq"), new Text("total_lines"),new IntWritable(acum), getConf());

        } catch (IOException e) {
            log.error("Error running job", e);
        } catch (InterruptedException e) {
            log.error("Error running job", e);
        } catch (ClassNotFoundException e) {
            log.error("Error running job", e);
        } catch (InstantiationException e) {
            log.error("Error running job", e);
        } catch (IllegalAccessException e) {
            log.error("Error running job", e);
        }
    }

    @Override
    public Path getOutputFile() {
        return outputFile;
    }

    @Override
    public Object getOutput() {
        try {
            return new Integer(((IntWritable) TableUtils.readFirstWritable(outputFile,getConf())[1]).get());
        } catch (IOException e) {
            log.error("Error getting output from job");
        } catch (IllegalAccessException e) {
            log.error("Error getting output from job");
        } catch (InstantiationException e) {
            log.error("Error getting output from job");
        }

        return null;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
