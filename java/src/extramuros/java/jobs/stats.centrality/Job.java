package extramuros.java.jobs.stats.centrality;

import extramuros.java.formats.AbstractTable;
import extramuros.java.formats.adapters.AbstractTableAdapter;
import extramuros.java.jobs.utils.ExtramurosJob;
import extramuros.java.jobs.utils.JobKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.HashMap;

/**
 * User: antonio
 * Date: 01/06/2011
 * Time: 16:31
 */

/**
 * The stats.centrality.JobOld class implements a Hadoop job that computes
 * the average, min value, max value and number of not null for a
 * column in a extramuros.java.extramuros.java.visualization.Table column.
 *
 * The extramuros.java.visualization.Table object and the name of the column whose stats will be
 * computed must be passed in the constructor of the class.
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

        public Path getOutputFile() {
            try{

                return getAllChildrenFiles(outputPath)[0];
            }catch(Exception e) {
                log.error("Error generating output file",e);
                return null;
            }

        }

        public Object getOutput() {
            Writable[] firstRow = readFirstRecord(getOutputFile());
            Vector v = ((VectorWritable) firstRow[1]).get();
            HashMap<String,Double> stats = new HashMap<String, Double>(4);
            stats.put("avg",v.get(0));
            stats.put("min",v.get(1));
            stats.put("max",v.get(2));
            stats.put("count",v.get(3));

            return stats;
        }

        public void run() {
            try {
                if(getFs().exists(outputPath)) {
                    getFs().delete(outputPath, true);
                }

                getConf().set(JobKeys.PATH, new Path(table.getTablePath()).toUri().getPath().toString());
                getConf().set(JobKeys.COLUMN_NAME, columnName);


                // prepare Hadoop job
                setJob(new org.apache.hadoop.mapreduce.Job(getConf(),"centrality stats job for column: "+columnName));

                getJob().setJobName("centrality_stats_job");

                getJob().setMapperClass(Mapper.class);
                getJob().setReducerClass(Reducer.class);

                getJob().setMapOutputKeyClass(Text.class);
                getJob().setMapOutputValueClass(DoubleWritable.class);

                getJob().setOutputKeyClass(Text.class);
                getJob().setOutputValueClass(VectorWritable.class);

                if(table.isAdapter()) {
                    getJob().setInputFormatClass(((AbstractTableAdapter<Writable,Writable>)table).inputFormat());
                } else {
                    getJob().setInputFormatClass(SequenceFileInputFormat.class);
                }
                getJob().setOutputFormatClass(SequenceFileOutputFormat.class);

                String inputPathString = composeInputPathString(new Path(table.getRowsPath()));
                FileInputFormat.setInputPaths(getJob(),inputPathString);
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
