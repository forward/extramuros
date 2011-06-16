package extramuros.java.jobs.clustering.proclus.algorithm;

import extramuros.java.jobs.utils.TableUtils;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;


/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 17:19
 */
public class ProClusClusterEvaluationJob extends AbstractJob {


    private Path input;
    private Path clusterSetInputFile;
    private int iteration;
    private Path output;
    private FileSystem fs;
    private double minDeviation;
    private double evaluationMetric;
    private ClusterSet clusterSet;

    private static final Logger log = LoggerFactory.getLogger(ProClusClusterEvaluationJob.class);


    public ProClusClusterEvaluationJob() throws IOException {
        if(getConf()==null) {
            setConf(new Configuration());
        }
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

        Option minDeviationOpt = obuilder.withLongName("minDeviation").withRequired(false).withArgument(
                abuilder.withName("minDeviation").withMinimum(0).withMaximum(1).create()).withDescription(
                "minimu deviation in the evaluation of clusters [0,1]").withShortName("md").create();



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

            minDeviation = Double.parseDouble((String) cmdLine.getValue(minDeviationOpt, "0.1"));


            commonInitialization();


        } catch (OptionException e) {
            log.error("Command-line option Exception", e);
            CommandLineUtil.printHelp(group);
            return false;
        }

        return true;
      }

    private void commonInitialization() throws IOException {
        output = output.suffix("/clusters_evaluation");
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
        return output.suffix("/evaluation.txt");
    }

    private double computeFinalMetric(ClusterSet set) throws IOException, IllegalAccessException, InstantiationException {
        //SequenceFile.Reader reader = new SequenceFile.Reader(fs,outputDir.suffix("/part-r-00000"),config);

        ArrayList<Double> acum = new ArrayList<Double>();
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();


        log.info("READING CLUSTERS FROM "+output.toUri().getPath());

        Iterator<Pair<Writable,Writable>> clusterIterator = TableUtils.directorySeqIterator(output, getConf());

        while(clusterIterator.hasNext()) {
            Pair<Writable,Writable> pair = clusterIterator.next();
            Cluster cluster = (Cluster) pair.getFirst();
            clusters.add(cluster);
            acum.add(((DoubleWritable)pair.getSecond()).get());
        }


        double metric = 0;
        double totalPoints = 0;
        for(int i=0; i<acum.size(); i++){
            Cluster cluster = clusters.get(i);
            metric = metric + (acum.get(i)/clusters.size()) * cluster.getNumPoints();
            set.addCluster(cluster);
            totalPoints = totalPoints + cluster.getNumPoints();
        }

        log.info("Computed metric: "+(metric/totalPoints));

        Collections.sort(clusters,new Comparator<Cluster>() {
            public int compare(Cluster clustera, Cluster clusterb) {
                if(clustera.getNumPoints() < clusterb.getNumPoints()) {
                    return 1;
                } else if(clustera.getNumPoints() > clusterb.getNumPoints()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });

        double threshold = (totalPoints / clusters.size()) * minDeviation;
        for(Cluster cluster : clusters) {
            if(cluster.getNumPoints()<threshold) {
                cluster.setBestCluster(false);
            } else {
                cluster.setBestCluster(true);
            }
        }

        Cluster cluster = clusters.get(clusters.size() - 1);
        cluster.setBestCluster(false);

        return metric / totalPoints;
    }

    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {

            run();

        } else {
            throw new Exception("Impossible to parse arguments");
        }

        return 0;
    }

    public int run(Path inputFile, Path clusterSetInputFile, Path outputDir, double minDeviation, int iteration) throws Exception {
        return this.run(getConf(),input,clusterSetInputFile,outputDir, minDeviation, iteration);
    };

    public int run(Configuration config, Path inputFile, Path clusterSetInputFile, Path outputDir, double minDeviation, int iteration) throws Exception {
        setConf(config);
        this.fs = FileSystem.get(config);
        this.input = inputFile;
        log.info("*** CHECKING INPUT FILE:");
        log.info(input.toUri().getPath());
        log.info("EXISTS? :"+fs.exists(input));
        this.clusterSetInputFile = clusterSetInputFile;
        this.output = outputDir;
        this.minDeviation = minDeviation;
        this.iteration = iteration;

        commonInitialization();

        run();

        return 0;
    }

    public void run() throws Exception {
        log.info("SETTING INPUT CLUSTER:"+clusterSetInputFile.toUri().toString());
        getConf().set(ProClusConfigKeys.SET_PATH,clusterSetInputFile.toUri().toString());

        log.info("*** RUNNING CLUSTERS EVALUATION - ITERATION "+iteration);

        // prepare Hadoop job
        Job job = new Job(getConf(), "Clusters evaluation algorithm execution, output: "+output.toUri().getPath()
                + " input:" + input.toUri().getPath());


        job.setMapperClass(ProClusPointsAssignmentMapper.class);
        job.setReducerClass(ProClusClusterEvaluationReducer.class);

        job.setMapOutputKeyClass(Cluster.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(Cluster.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        String inputFiles = ProClusUtils.composeInputPathString(fs, input);
        FileInputFormat.addInputPaths(job, inputFiles);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(ProClusClusterEvaluationJob.class);


        if (!job.waitForCompletion(true)) {
            throw new InterruptedException("Clusters evaluation algorithm failed processing " + input.toUri().getPath()
                    + " output: " + output.toUri().getPath());
        }

        ProClusUtils.cleanOutput(fs, output);

        setClusterSet(new ClusterSet());
        setEvaluationMetric(computeFinalMetric(getClusterSet()));

        log.info(" EVALUATION METRIC: " + getEvaluationMetric() + " - " + getClusterSet().bestClustersCount() + " best clusters found");

        writeOutputSequence();
    }

    private void writeOutputSequence() throws IOException, IllegalAccessException, InstantiationException {
        Path outputFileIteration = output.suffix("/evaluation.txt");
        log.info("Writing evaluation sequence file in: "+outputFileIteration.toUri().getPath());
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,getConf(),outputFileIteration,ClusterSet.class,DoubleWritable.class);

        try {
            writer.append(getClusterSet(), new DoubleWritable(getEvaluationMetric()));
        } finally {
            IOUtils.quietClose(writer);
        }
    }

    public static void main(String[] args) {
        try {
            ProClusClusterEvaluationJob job = new ProClusClusterEvaluationJob();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running cluster evaluation job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public double getEvaluationMetric() {
        return evaluationMetric;
    }

    public void setEvaluationMetric(double evaluationMetric) {
        this.evaluationMetric = evaluationMetric;
    }

    public ClusterSet getClusterSet() {
        return clusterSet;
    }

    public void setClusterSet(ClusterSet clusterSet) {
        this.clusterSet = clusterSet;
    }
}
