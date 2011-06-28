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
public class ProClusDimensionSelectionJob extends AbstractJob {

    protected FileSystem fs;
    protected Path input;
    protected Path medoidSetInputFile;
    protected Path outputDir;

    // Factor of scale for the number of dimensions to select = kl = num_medoids * l
    protected int l;

    // Current medoid set whose dimensions is going to be selected
    MedoidSet medoidSet;

    private static final Logger log = LoggerFactory.getLogger(ProClusDimensionSelectionJob.class);

    public ProClusDimensionSelectionJob() throws IOException {
        if(getConf() == null) {
            setConf(new Configuration());
         }
        fs = FileSystem.get(getConf());
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
            "The input directory").withShortName("i").create();

        Option medoidSetinputFileOpt = obuilder.withLongName("medoidSetinputFile").withRequired(true).withArgument(
            abuilder.withName("medoidSetInputFile").withMinimum(1).withMaximum(1).create()).withDescription(
            "The file containing the medoid set").withShortName("mi").create();

        Option outputDirOpt = obuilder.withLongName("outputDir").withRequired(true).withArgument(
                abuilder.withName("outputDir").withMinimum(1).withMaximum(1).create()).withDescription(
                "The data output directory").withShortName("o").create();

        Option lOpt = obuilder.withLongName("l").withRequired(true).withArgument(
                abuilder.withName("l").create()).withDescription(
                "Num of dimensions scale factor").withShortName("l").create();


        Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt).
                withOption(medoidSetinputFileOpt).withOption(lOpt).create();

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
            medoidSetInputFile = new Path((String) cmdLine.getValue(medoidSetinputFileOpt));
            outputDir = new Path((String) cmdLine.getValue(outputDirOpt));


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

        outputDir = outputDir.suffix("/dimension_selection");

        // mkdir for dimension selection phase
        // {outputDir}/dimension_selection
        if(fs.exists(outputDir)) {
            fs.delete(outputDir,true);
        }
        fs.mkdirs(outputDir);
    }

    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {
            run();

        } else {
            throw new Exception("Impossible to parse arguments");
        }

        return 0;
    }


    public int run(Path input, Path medoidSetInputFile, Path outputDir, int l) throws Exception {
        return run(getConf(),input,medoidSetInputFile,outputDir, l);
    }

    public int run(Configuration config, Path input, Path medoidSetInputFile, Path outputDir, int l) throws Exception {
        setConf(config);
        fs = FileSystem.get(getConf());
        this.input = input;
        this.medoidSetInputFile = medoidSetInputFile;
        this.outputDir = outputDir;
        this.l = l;

        commonInitialization();

        run();

        return 0;
    }

    public void run() throws Exception {
        // read the medoid set
        //log.info("*** READING MEDOIDS FROM -> "+medoidSetInputFile.toUri().getPath());
        //medoidSet = readMedoidSet(medoidSetInputFile);

        // write the initial medoid set
        //Path medoidSetPath = writeInitialInitialMedoidSet(medoidSet);
        //log.info("*** PATH -> "+medoidSetPath.toUri().getPath());

        // run the job
        log.info("*** PATH -> "+medoidSetInputFile.toUri().getPath());
        runSelectDimensions(medoidSetInputFile);

        // sort the final set of initial clusters
        buildInitialClusters();
    }


    public Path getOutputFile() throws IOException {
        return pathForOutputData(false).suffix("_clusters.txt");
    }

    private void buildInitialClusters() throws IOException, IllegalAccessException, InstantiationException {
        Path medoidsInputFile = pathForOutputData(false);//.suffix("/part-r-00000");
        //SequenceFile.Reader reader = new SequenceFile.Reader(fs,medoidsInputFile,getConf());
        Path output = pathForOutputData(false).suffix("_clusters.txt");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, getConf(),output, IntWritable.class, ClusterSet.class);
        try {
            MedoidSet set = new MedoidSet();

            Iterator<Pair<Writable,Writable>> medoidsIterator = TableUtils.directorySeqIterator(medoidsInputFile,getConf());
            while(medoidsIterator.hasNext()) {
                Pair<Writable,Writable> pair = medoidsIterator.next();
                Medoid medoid = (Medoid) pair.getSecond();
                set.addMedoid(medoid);
            };

            log.info("RETRIEVED "+set.count()+" MEDOIDS");

            // we compute the initial dimensions for each medoid
            log.info("COMPUTING DIMENSIONS WITH L:"+l);
            set.computeDimensions(l);


            for(Medoid tmp : set.getMedoids()) {
                log.info("MEDOID: "+tmp.getLabel());
                for(int i=0; i<tmp.getDimensions().length; i++) {
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

    private void runSelectDimensions(Path medoidSetPath) throws IOException, ClassNotFoundException, InterruptedException {
        log.info("*** RUNNING DIMENSION SELECTION");

        // deal with paths
        String inputIterationPath  = ProClusUtils.composeInputPathString(fs, input);
        Path outputIterationPath = pathForOutputData(true);


        // prepare Hadoop job
        log.info("SETTING INPUT MEDOID SET:"+medoidSetPath.toUri().toString());
        getConf().set(ProClusConfigKeys.SET_PATH,medoidSetPath.toUri().toString());

        Job job = new Job(getConf(), "Dimension selection algorithm execution, output: "+outputIterationPath.toUri().getPath()
                + " input:" + inputIterationPath);


        job.setMapperClass(ProClusDimensionSelectionMapper.class);
        job.setReducerClass(ProClusDimensionSelectionReducer.class);

        job.setMapOutputKeyClass(Medoid.class);
        job.setMapOutputValueClass(MedoidDimensionalData.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Medoid.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPaths(job,inputIterationPath);
        FileOutputFormat.setOutputPath(job, outputIterationPath);

        job.setJarByClass(ProClusDimensionSelectionJob.class);

        if (!job.waitForCompletion(true)) {
          throw new InterruptedException("Dimension selection algorithm failed processing " + inputIterationPath
                  + " output: " + outputIterationPath.toUri().getPath());
        }

        ProClusUtils.cleanOutput(fs,outputIterationPath);
    }

    private Path pathForInputData() {
        return outputDir.suffix("/input.txt");
    }

    private Path pathForOutputData(boolean recreate) throws IOException {
        Path path = outputDir.suffix("/output");
        if(recreate) {
            if(fs.exists(path)) {
                fs.delete(path,true);
            }
        }

        return path;
    }

    private MedoidSet readMedoidSet(Path input) throws IOException, IllegalAccessException, InstantiationException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,input,getConf());
        MedoidSet set = new MedoidSet();

        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            reader.next(key,set);
        } finally {
            IOUtils.quietClose(reader);
        }

        log.info("Read initial medoid set:"+set+" with "+set.count()+" medoids from file: "+medoidSetInputFile.toUri().getPath());
        return set;
    }

    private Path writeInitialInitialMedoidSet(MedoidSet medoidSet) throws IOException, IllegalAccessException, InstantiationException {
        Path outputFileIteration = pathForInputData();
        log.info("Writing initial sequence file in: "+outputFileIteration.toUri().getPath());
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,getConf(),outputFileIteration,IntWritable.class,MedoidSet.class);


        try {
            writer.append(new IntWritable(medoidSet.count()),medoidSet);
        } finally {
            IOUtils.quietClose(writer);
        }

        return outputFileIteration;
    }

    public static void main(String[] args) {
        try {
            ProClusDimensionSelectionJob job = new ProClusDimensionSelectionJob();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running dimension selection job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
