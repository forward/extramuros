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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
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
public class ProClusPointsClusteringJob extends AbstractJob {

    protected FileSystem fs;
    protected Path input;
    protected Path clusterSetInputFile;
    protected Path output;

    private static final Logger log = LoggerFactory.getLogger(ProClusPointsClusteringJob.class);


    public ProClusPointsClusteringJob() throws IOException {
        if(getConf()==null) {
            setConf(new Configuration());
        }
        fs = FileSystem.get(getConf());
    }

      public boolean parseArgs(String[] args) throws Exception {

        DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
        ArgumentBuilder abuilder = new ArgumentBuilder();
        GroupBuilder gbuilder = new GroupBuilder();
        Option helpOpt = DefaultOptionCreator.helpOption();

        Option inputFileOpt = obuilder.withLongName("inputFile").withRequired(true).withArgument(
            abuilder.withName("inputFile").withMinimum(1).withMaximum(1).create()).withDescription(
            "The input file").withShortName("i").create();

        Option clusterSetinputFileOpt = obuilder.withLongName("clustersInputFile").withRequired(true).withArgument(
            abuilder.withName("clustersInputFile").withMinimum(1).withMaximum(1).create()).withDescription(
            "The file containing the clusters set").withShortName("ci").create();

        Option outputDirOpt = obuilder.withLongName("outputDir").withRequired(true).withArgument(
                abuilder.withName("outputDir").withMinimum(1).withMaximum(1).create()).withDescription(
                "The data output directory").withShortName("o").create();



        Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt).
                withOption(clusterSetinputFileOpt).create();

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

        output = output.suffix("/clusteredPoints");

        // mkdir for dimension selection phase
        // {outputDir}/points_assignment/itearation-{iteration}
        if(fs.exists(output)) {
            fs.delete(output, true);
        }

    }

    public Path getOutput() {
        return output;
    }

    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {

            run();

        } else {
            throw new Exception("Impossible to parse arguments");
        }

        return 0;
    }

    public void run(Configuration config, Path input, Path clusterSetInputFile, Path output) throws Exception {
        setConf(config);
        this.fs = FileSystem.get(getConf());
        this.input = input;
        this.clusterSetInputFile = clusterSetInputFile;
        this.output  =  output;

        commonInitialization();

        run();
    }

    public void run() throws Exception {
        log.info("SETTING INPUT CLUSTER:"+clusterSetInputFile.toUri().toString());
        getConf().set(ProClusConfigKeys.SET_PATH,clusterSetInputFile.toUri().toString());

        log.info("*** RUNNING POINTS CLUSTERING ");

        // prepare Hadoop job
        Job job = new Job(getConf(), "Points clustering algorithm execution, output: "+output.toUri().getPath()
                + " input:" + input.toUri().getPath());


        job.setMapperClass(ProClusPointsAssignmentMapper.class);
        job.setReducerClass(ProClusPointsClusteringReducer.class);

        job.setMapOutputKeyClass(Cluster.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        String inputFiles = ProClusUtils.composeInputPathString(fs, input);
        FileInputFormat.addInputPaths(job, inputFiles);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(ProClusPointsAssignmentJob.class);


        if (!job.waitForCompletion(true)) {
            throw new InterruptedException("Points clustering algorithm failed processing " + input.toUri().getPath()
                    + " output: " + output.toUri().getPath());
        }

        ProClusUtils.cleanOutput(fs, output);
    }

    public static void main(String[] args) {
        try {
            ProClusPointsClusteringJob job = new ProClusPointsClusteringJob();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running cluster points assignment job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
