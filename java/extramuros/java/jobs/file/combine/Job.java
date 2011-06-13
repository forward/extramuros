package extramuros.java.jobs.file.combine;

import extramuros.java.jobs.utils.ExtramurosJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * User: antonio
 * Date: 02/06/2011
 * Time: 17:12
 */
public class Job extends ExtramurosJob {

    protected Path inputDirectory;
    protected Path outputFile;

    private static final Logger log = LoggerFactory.getLogger(Job.class);

    public Job(Path inputDirectory, Path outputFile, Configuration configuration) throws IOException {
        super(configuration);
        this.inputDirectory = inputDirectory;
        this.outputFile = outputFile;
    }

    @Override
    public void run() {
        SequenceFile.Writer outWriter = null;
        try {
            Path[] filesToCombine = getAllChildrenFiles(inputDirectory);
            if(filesToCombine.length > 1) {
                for(Path fileToCombine : filesToCombine) {
                    SequenceFile.Reader reader = new SequenceFile.Reader(getFs(), fileToCombine, getConf());

                    if(outWriter == null) {
                        outWriter = new SequenceFile.Writer(getFs(), getConf(), outputFile,
                                reader.getKeyClass().asSubclass(Writable.class),
                                reader.getValueClass().asSubclass(Writable.class));

                    }

                    try {
                        Writable key = (Writable) reader.getKeyClass().newInstance();
                        Writable value = (Writable) reader.getValueClass().newInstance();

                        while (reader.next(key, value)) {
                            outWriter.append(key, value);
                        }

                    } finally {
                        IOUtils.quietClose(reader);
                        getFs().delete(fileToCombine,true);
                    }
                }
            } else {
              getFs().rename(filesToCombine[0],outputFile);
            }
        } catch (Exception e) {
            log.error("Error writing combined file " + outputFile.toUri().getPath(), e);
        } finally {
            if(outWriter!=null)
                IOUtils.quietClose(outWriter);
        }
    }

    @Override
    public Path getOutputFile() {
        return outputFile;
    }

    @Override
    public Object getOutput() {
        return outputFile;
    }

    public int run(String[] strings) throws Exception {
        run();
        return 0;
    }
}
