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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.IOUtils;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 24/05/2011
 * Time: 10:27
 */
public class ProClusGreedyInitializationJob extends AbstractJob {

    private Path inputFile;
    private Path outputDir;
    private Path greedyOutputDir;
    private FileSystem fs;
    private int kValue;

    private static final Logger log = LoggerFactory.getLogger(ProClusGreedyInitializationJob.class);

    public ProClusGreedyInitializationJob() throws IOException {
        if(getConf() == null) {
            setConf(new Configuration());
         }
        fs = FileSystem.get(getConf());
    }

    /**
     * Parse the arguments for the job:
     *  - sampleFile: file with the initial data points sample
     *  - outputDir: directory where the first medoidset/points fille must be written
     */
      public boolean parseArgs(String[] args) throws Exception {

        DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
        ArgumentBuilder abuilder = new ArgumentBuilder();
        GroupBuilder gbuilder = new GroupBuilder();
        Option helpOpt = DefaultOptionCreator.helpOption();

        Option inputFileOpt = obuilder.withLongName("inputFile").withRequired(true).withArgument(
            abuilder.withName("inputFile").withMinimum(1).withMaximum(1).create()).withDescription(
            "The input file").withShortName("i").create();

        Option outputDirOpt = obuilder.withLongName("outputDir").withRequired(true).withArgument(
                abuilder.withName("outputDir").withMinimum(1).withMaximum(1).create()).withDescription(
                "The data output directory").withShortName("o").create();

        Option kValueOpt = obuilder.withLongName("k").withRequired(true).withArgument(
                abuilder.withName("k").withMinimum(1).create()).withDescription(
                "K number of initial medoids to find (k>num clusters)").withShortName("k").create();



        Group group = gbuilder.withName("Options").withOption(inputFileOpt).withOption(outputDirOpt).
                withOption(kValueOpt).create();

        try {

            Parser parser = new Parser();
            parser.setGroup(group);
            CommandLine cmdLine = parser.parse(args);

            if (cmdLine.hasOption(helpOpt)) {
                CommandLineUtil.printHelp(group);
                return false;
            }

            kValue = Integer.parseInt((String) cmdLine.getValue(kValueOpt));
            inputFile = new Path((String) cmdLine.getValue(inputFileOpt));
            outputDir = new Path((String) cmdLine.getValue(outputDirOpt));

            commonInitialization();


        } catch (OptionException e) {
            log.error("Command-line option Exception", e);
            CommandLineUtil.printHelp(group);
            return false;
        }

        return true;
      }

    private void commonInitialization() throws IOException {
        if(!fs.exists(outputDir)) {
            fs.mkdirs(outputDir);
        }


        // mkdir for greedy initial phase
        // {outputDir}/greedy
        greedyOutputDir = outputDir.suffix("/greedy");
        fs.mkdirs(greedyOutputDir);
    }

    public Path getOutputFile() {
        return greedyOutputDir.suffix("/medoids-"+(kValue-1)+"/part-r-00000");
    }

    // start from command line
    public int run(String[] args) throws Exception {
        if(parseArgs(args)) {
            run();
        } else {
          throw new Exception("Impossible to parse arguments");
        }

        return 0;
    }

    // start from a different object
    public int run(Configuration config, Path inputFile, Path outputDir, int kValue) throws Exception {
        setConf(config);
        this.fs = FileSystem.get(getConf());
        this.inputFile = inputFile;
        this.outputDir = outputDir;
        this.kValue = kValue;

        commonInitialization();

        run();

        return 0;
    }

    public int run(Path inputFile, Path outputDir, int kValue) throws Exception {
        this.inputFile = inputFile;
        this.outputDir = outputDir;
        this.kValue = kValue;

        commonInitialization();

        run();

        return 0;
    }


    // common run logic
    public void run() throws Exception {
        // sort the input for the greedy algorithm
        buildFirstMedoidSet();

        // run the greedy initialization
        for(int i=1; i<kValue; i++) {
            runGreedy(i);
        }
    }

    private void runGreedy(int iteration) throws IOException, ClassNotFoundException, InterruptedException {
        log.info("*** GREEDY INITIAL PHASE: STARTING ITERATION "+iteration);
        getConf().set(ProClusConfigKeys.ITERATION_NUM_KEY, "" + iteration);
        getConf().set(ProClusConfigKeys.SET_PATH, inputFileForIteration(iteration-1, false).toUri().getPath());
        log.info(" ** PATH:"+inputFileForIteration(iteration-1, false).toUri().getPath());
        log.info(" ** EXISTS? "+fs.exists(inputFileForIteration(iteration-1,false)));
        // deal with paths

        Path inputIterationPath  = inputFile;
        Path outputIterationPath = fileForIteration(iteration, true);


        // prepare Hadoop job
        log.info(" ** CONFIG JOB:");
        log.info(getConf().toString());
        Job job = new Job(getConf(), "Greedy initialization algorithm iteration: " + iteration + " output: "+
                outputIterationPath.toUri().getPath() + " input:" + inputIterationPath.toUri().getPath());


        job.setMapperClass(ProClusGreedyInitializationMapper.class);
        job.setReducerClass(ProClusGreedyInitializationReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(MedoidSet.class);
        job.setMapOutputValueClass(WeightedVectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MedoidSet.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        String inputPathString = ProClusUtils.composeInputPathString(fs,inputIterationPath);
        log.info("Composed input path "+inputPathString);
        FileInputFormat.addInputPaths(job, inputPathString);
        FileOutputFormat.setOutputPath(job, outputIterationPath);

        job.setJarByClass(ProClusGreedyInitializationJob.class);

        if (!job.waitForCompletion(true)) {
          throw new InterruptedException("Greedy initialization algorithm iteration :" + iteration + " failed processing " + inputIterationPath.toUri().getPath() + " output: " + outputIterationPath.toUri().getPath());
        }

        ProClusUtils.cleanOutput(fs,outputIterationPath);
    }

    private Path fileForIteration(int i, boolean shouldDestroy) throws IOException {
        Path file = greedyOutputDir.suffix("/medoids-"+ i);

        if(shouldDestroy) {
            if(fs.exists(file)) {
                log.info("DELETING DIRECTORY:"+file.toUri().getPath());
                fs.delete(file,true);
            }
            //fs.mkdirs(file);
        }

        //file = file.suffix("/medoids.txt");

        return file;
    }

    private Path inputFileForIteration(int i, boolean shouldDestroy) throws IOException {
        Path tmp  = fileForIteration(i,shouldDestroy).suffix("/part-r-00000");
        return tmp;
    }

    private void buildFirstMedoidSet() throws Exception {
        Vector vector = ((VectorWritable) readFirstElement(inputFile, VectorWritable.class)).get();
        Medoid medoid = new Medoid(vector);
        Medoid[] medoidArray = { medoid };
        MedoidSet medoidSet = new MedoidSet(medoidArray);

        Path outputFileIteration = inputFileForIteration(0,true);
        log.info("Writing intial medoid set to file: " + outputFileIteration.toUri().getPath());

        SequenceFile.Writer writer = new SequenceFile.Writer(fs,getConf(),outputFileIteration, Text.class, MedoidSet.class);

        try {
            writer.append(new Text("IT-0"),medoidSet);
        } finally {
            IOUtils.quietClose(writer);
        }
    }

    private Writable readFirstElement(Path input, Class kls) throws IOException, IllegalAccessException, InstantiationException {
        Path firstInputfile = input;
        if(!fs.isFile(firstInputfile)) {
            firstInputfile = fs.listStatus(firstInputfile)[0].getPath();
        }
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,firstInputfile,getConf());
        Writable value = (Writable) kls.newInstance();
        try {
            Writable key = (Writable) reader.getKeyClass().newInstance();
            reader.next(key,value);
        } finally {
            IOUtils.quietClose(reader);
        }
        log.info("Generated first vector: "+value);
        return value;
    }

    public static void main(String[] args) {
        try {
            ProClusGreedyInitializationJob job = new ProClusGreedyInitializationJob();
            job.run(args);
        } catch (Exception e) {
            System.out.println("Error running greedy initialization first job");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
