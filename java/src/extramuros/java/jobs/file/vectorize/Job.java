package extramuros.java.jobs.file.vectorize;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.RowTypes;
import extramuros.java.formats.TableHeader;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.formats.adapters.VectorSeqTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 16:50
 */
public class Job extends ExtramurosJob {

    protected Path outputPath;
    protected AbstractTable table;
    protected String [] columns;
    protected Class< ? extends Vector> vectorClass;

    private static final Logger log = LoggerFactory.getLogger(Job.class);


    public Job(String outputPath, String[] columns, Class< ? extends Vector> vectorClass, AbstractTable table, Configuration configuration) throws IOException {
        super(configuration);
        this.columns = columns;
        this.table = table;
        this.outputPath = new Path(outputPath);
        this.vectorClass = vectorClass;
    }

    @Override
    public void run() {
        try {
            if (getFs().exists(outputPath)) {
                getFs().delete(outputPath, true);
            }

            getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
            StringBuilder columnsBuilder = new StringBuilder(columns[0]);
            for(int i=1; i<columns.length; i++) {
                columnsBuilder.append(","+columns[i]);
            }
            getConf().set(JobKeys.COLUMNS, columnsBuilder.toString());
            getConf().set(JobKeys.CLASS_NAME, vectorClass.getName());

            // prepare Hadoop job
            setJob(new org.apache.hadoop.mapreduce.Job(getConf(), "vectorize job. "));

            getJob().setJobName("vectorize_job");

            getJob().setMapperClass(Mapper.class);
            getJob().setNumReduceTasks(0);

            getJob().setMapOutputKeyClass(IntWritable.class);
            getJob().setMapOutputValueClass(VectorWritable.class);

            getJob().setOutputKeyClass(IntWritable.class);
            getJob().setOutputValueClass(VectorWritable.class);


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
        return outputPath;
    }

    @Override
    public Object getOutput() {
        ArrayList<String> newColumnNames = new ArrayList<String>(this.columns.length);
        ArrayList<Integer> newColumnTypes = new ArrayList<Integer>(this.columns.length);

        for(String columnName : columns) {
            newColumnNames.add(columnName);
            newColumnTypes.add(RowTypes.DOUBLE);
        }
        TableHeader header = new TableHeader(newColumnNames, newColumnTypes);
        VectorSeqTableAdapter table = new VectorSeqTableAdapter(header,outputPath.toUri().getPath());

        table.setConfiguration(getConf());

        return table;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
