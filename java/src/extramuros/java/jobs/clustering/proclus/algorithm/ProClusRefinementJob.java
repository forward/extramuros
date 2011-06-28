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
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;


/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 09:47
 */
public class ProClusRefinementJob extends AbstractJob {

    protected FileSystem fs;
    protected Path input;
    protected Path clusterSetInputFile;
    protected Path output;

    // Factor of scale for the number of dimensions to select = kl = num_medoids * l
    protected int l;

    private static final Logger log = LoggerFactory.getLogger(ProClusRefinementJob.class);

    public ProClusRefinementJob() throws IOException {
        if(getConf()==null) {
            setConf(new Configuration());
        }
    }

     /**
      * Parse the arguments for the job:
      *  - input: file containing a sequence of <MedoidSet,Points>
      *  - outputDir: directory where the result will be written
      */
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

        Option lOpt = obuilder.withLongName("l").withRequired(true).withArgument(
                abuilder.withName("l").create()).withDescription(
                "Num of dimensions scale factor").withShortName("l").create();


        Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt).
                withOption(lOpt).withOption(clusterSetinputFileOpt).create();

        try {

            Parser parser = new Parser();
            parser.setGroup(group);
            CommandLine cmdLine = parser.parse(args);

            if (cmdLine.hasOption(helpOpt)) {
                CommandLineUtil.printHelp(group);
                return false;
            }

            l = Integer.parseInt((String) cmdLine.getValue(lOpt));

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

    public void commonInitialization() throws IOException {
        // Recreated in each iteration

        output = output.suffix("/result");

        // mkdir for dimension selection phase
        // {outputDir}/dimension_selection
        if(fs.exists(output)) {
            fs.delete(output,true);
        }
        fs.mkdirs(output);
    }

    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {
            run();

        } else {
            throw new Exception("Impossible to parse arguments");
        }

        return 0;
    }


    public int run(Path input, Path clusterSetInputFile, Path output, int l) throws Exception {
        return run(getConf(), input, clusterSetInputFile, output, l);
    }

    public int run(Configuration config, Path input, Path clusterSetInputFile, Path output, int l) throws Exception {
        setConf(config);
        this.fs = FileSystem.get(getConf());
        this.input = input;
        this.clusterSetInputFile = clusterSetInputFile;
        this.output = output;
        this.l = l;

        commonInitialization();

        run();

        return 0;
    }

    public void run() throws Exception {
        log.info("SETTING INPUT CLUSTER:"+clusterSetInputFile.toUri().toString());
        getConf().set(ProClusConfigKeys.SET_PATH,clusterSetInputFile.toUri().toString());

        // run the job
        runSelectDimensions();

        // sort the final set of initial clusters
        buildInitialClusters();
    }


    public Path getOutputFile() throws IOException {
        return output.suffix("/clusters.txt");
    }

    private void buildInitialClusters() throws IOException, IllegalAccessException, InstantiationException {
        //Path medoidsInputFile = pathForOutputData(false).suffix("/part-r-00000");
        //SequenceFile.Reader reader = new SequenceFile.Reader(fs,medoidsInputFile,config);
        Path output = this.output.suffix("/clusters.txt");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, getConf(),output, IntWritable.class, ClusterSet.class);
        try {
            MedoidSet set = new MedoidSet();

            Iterator<Pair<Writable,Writable>> medoidIterator = TableUtils.directorySeqIterator(pathForOutputData(false), getConf());
            while(medoidIterator.hasNext()) {
                Medoid medoid = (Medoid) medoidIterator.next().getSecond();

                set.addMedoid(medoid);
            }

            log.info("RETRIEVED "+set.count()+" MEDOIDS");

            // we compute the initial dimensions for each medoid
            log.info("COMPUTING DIMENSIONS WITH L:"+l);
            set.computeDimensions(l);


            for(Medoid tmp : set.getMedoids()) {
                log.info("MEDOID: "+tmp.getLabel());
                for(int i=0; i< tmp.getDimensions().length; i++) {
                    log.info(" - DIM:"+ tmp.getDimensions()[i]);
                }
                log.info("\n\n");
            }

            // creation of the initial clusters
            ClusterSet clusters = new ClusterSet();
            Medoid[] medoids = set.getMedoids();
            for(Medoid out : medoids){
                out.clearWeightedDimensions();
                clusters.addCluster(new Cluster(out));
            }

            // writing initial clusters to HDFS

            writer.append(new IntWritable(clusters.count()), clusters);

        } finally {
            IOUtils.quietClose(writer);
        }

    }

    private void runSelectDimensions() throws IOException, ClassNotFoundException, InterruptedException {
        if(getConf() == null) {
            setConf(new Configuration());
         }

        log.info("*** RUNNING DIMENSION SELECTION");

        // deal with paths
        Path inputIterationPath  = input;
        Path outputIterationPath = pathForOutputData(true);


        // prepare Hadoop job
        Job job = new Job(getConf(), "Dimension selection algorithm execution, output: "+outputIterationPath.toUri().getPath()
                + " input:" + inputIterationPath.toUri().getPath());


        job.setMapperClass(ProClusRefinementMapper.class);
        job.setReducerClass(ProClusDimensionSelectionReducer.class);

        job.setMapOutputKeyClass(Medoid.class);
        job.setMapOutputValueClass(MedoidDimensionalData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Medoid.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //FileInputFormat.addInputPath(job, inputIterationPath);
        FileInputFormat.addInputPaths(job, ProClusUtils.composeInputPathString(fs, inputIterationPath));
        FileOutputFormat.setOutputPath(job, outputIterationPath);

        job.setJarByClass(ProClusRefinementJob.class);

        if (!job.waitForCompletion(true)) {
          throw new InterruptedException("Refinement algorithm failed processing " + inputIterationPath.toUri().getPath()
                  + " output: " + outputIterationPath.toUri().getPath());
        }

        ProClusUtils.cleanOutput(fs, outputIterationPath);
    }


    private Path pathForOutputData(boolean recreate) throws IOException {
        Path path = output.suffix("/output");
        if(recreate) {
            if(fs.exists(path)) {
                fs.delete(path,true);
            }
        }

        return path;
    }


    public static void main(String[] args) {
        try {
            ProClusRefinementJob job = new ProClusRefinementJob();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running dimension selection job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
