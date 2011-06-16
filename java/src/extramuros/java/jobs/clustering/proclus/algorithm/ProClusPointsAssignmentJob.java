package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.contrib.mr.Output;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 15:30
 */
public class ProClusPointsAssignmentJob extends AbstractJob {

    protected Configuration config;
    protected FileSystem fs;
    protected Path input;
    protected Path clusterSetInputFile;
    protected Path output;
    protected int iteration;

    private static final Logger log = LoggerFactory.getLogger(ProClusPointsAssignmentJob.class);


    public ProClusPointsAssignmentJob() throws IOException {
        config = new Configuration();
        if(getConf()==null) {
            setConf(config);
        }
        fs = FileSystem.get(config);
    }

      public boolean parseArgs(String[] args) throws Exception {

        DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
        ArgumentBuilder abuilder = new ArgumentBuilder();
        GroupBuilder gbuilder = new GroupBuilder();
        Option helpOpt = DefaultOptionCreator.helpOption();

        Option inputFileOpt = obuilder.withLongName("input").withRequired(true).withArgument(
            abuilder.withName("input").withMinimum(1).withMaximum(1).create()).withDescription(
            "The input file").withShortName("i").create();

        Option clusterSetinputFileOpt = obuilder.withLongName("clustersInputFile").withRequired(true).withArgument(
            abuilder.withName("clustersInputFile").withMinimum(1).withMaximum(1).create()).withDescription(
            "The file containing the clusters set").withShortName("ci").create();

        Option outputDirOpt = obuilder.withLongName("output").withRequired(true).withArgument(
                abuilder.withName("output").withMinimum(1).withMaximum(1).create()).withDescription(
                "The data output directory").withShortName("o").create();

        Option iterationOpt = obuilder.withLongName("iteration").withRequired(true).withArgument(
                abuilder.withName("iteration").withMinimum(1).withMaximum(1).create()).withDescription(
                "number of iteration").withShortName("it").create();



        Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt).
                withOption(clusterSetinputFileOpt).withOption(iterationOpt).create();

        try {

            Parser parser = new Parser();
            parser.setGroup(group);
            CommandLine cmdLine = parser.parse(args);

            if (cmdLine.hasOption(helpOpt)) {
                CommandLineUtil.printHelp(group);
                return false;
            }


            input = new Path((String) cmdLine.getValue(inputFileOpt));
            clusterSetInputFile = new Path((String) cmdLine.getValue(clusterSetinputFileOpt));
            iteration = Integer.parseInt(((String) cmdLine.getValue(iterationOpt)));
            output = new Path((String) cmdLine.getValue(outputDirOpt));

            commonInitialization();

        } catch (OptionException e) {
            log.error("Command-line option Exception", e);
            CommandLineUtil.printHelp(group);
            return false;
        }

        return true;
      }

    private void commonInitialization() throws IOException{

        output = output.suffix("/points_assignment");

        // mkdir for dimension selection phase
        // {outputDir}/points_assignment/itearation-{iteration}
        if(fs.exists(output) && iteration==0) {
            fs.delete(output,true);
            fs.mkdirs(output);
        }

        output = output.suffix("/iteration-"+iteration);
        if(fs.exists(output)) {
            fs.delete(output,true);
        }
    }

    public Path getOutputFile() {
        return output.suffix("/clusters.txt");
    }

    private void buildNextIterationClusters() throws IOException, IllegalAccessException, InstantiationException {
        Path output = this.output.suffix("/clusters.txt");
        Path medoidsInputFile = this.output.suffix("/part-r-00000");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,medoidsInputFile,config);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, config,output, IntWritable.class, ClusterSet.class);
        try {
            ClusterSet clusters = new ClusterSet();

            Writable key = (Writable) reader.getKeyClass().newInstance();
            Cluster cluster = new Cluster();


            while(reader.next(key,cluster)) {
                clusters.addCluster(cluster);
                cluster = new Cluster();
            };


            // writing initial clusters to HDFS
            writer.append(new IntWritable(clusters.count()), clusters);

        } finally {
            IOUtils.quietClose(reader);
            IOUtils.quietClose(writer);
        }

    }
    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {

            run();

        } else {
            throw new Exception("Impossible to parse arguments");
        }

        return 0;
    }

    public void run(Path input, Path clustersInputFile, Path output, int iteration) throws Exception {
        run(getConf(),input,clustersInputFile, output, iteration);
    };

    public void run(Configuration config, Path input, Path clusterSetInputFile, Path output, int iteration) throws Exception {
        setConf(config);
        fs = FileSystem.get(config);
        this.input = input;
        this.clusterSetInputFile = clusterSetInputFile;
        this.output  =  output;
        this.iteration = iteration;

        commonInitialization();

        run();
    }

    public void run() throws Exception {
        log.info("SETTING INPUT CLUSTER:"+clusterSetInputFile.toUri().toString());
        getConf().set(ProClusConfigKeys.SET_PATH,clusterSetInputFile.toUri().toString());

        log.info("*** RUNNING POINTS ASSIGNMENT - ITERATION "+iteration);

        // prepare Hadoop job
        Job job = new Job(getConf(), "Points assignement algorithm execution, output: "+output.toUri().getPath()
                + " input:" + input.toUri().getPath());


        job.setMapperClass(ProClusPointsAssignmentMapper.class);
        job.setReducerClass(ProClusPointsAssignmentReducer.class);

        job.setMapOutputKeyClass(Cluster.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Cluster.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        String inputFiles = ProClusUtils.composeInputPathString(fs, input);
        FileInputFormat.addInputPaths(job, inputFiles);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(ProClusPointsAssignmentJob.class);


        if (!job.waitForCompletion(true)) {
            throw new InterruptedException("Points assignation algorithm failed processing " + input.toUri().getPath()
                    + " output: " + output.toUri().getPath());
        }

        ProClusUtils.cleanOutput(fs, output);

        buildNextIterationClusters();
    }

    public static void main(String[] args) {
        try {
            ProClusPointsAssignmentJob job = new ProClusPointsAssignmentJob();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running cluster points assignment job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
