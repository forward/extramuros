package extramuros.java.jobs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * User: antonio
 * Date: 02/06/2011
 * Time: 09:02
 */
public abstract class ExtramurosJob extends AbstractJob {

    private FileSystem fs;
    private org.apache.hadoop.mapreduce.Job job;

    private static final Logger log = LoggerFactory.getLogger(ExtramurosJob.class);

    public ExtramurosJob(Configuration configuration) throws IOException {
        this.setConf(configuration);
        setFs(FileSystem.get(configuration));
    }


    public Writable[] readFirstRecord(Path file) {
        try {
            return TableUtils.readFirstWritable(file,getConf());
        } catch (IOException e) {
            log.error("Error reading first row from: "+file.toUri().getPath().toString(),e);
            return null;
        } catch (IllegalAccessException e) {
            log.error("Error reading first row from: "+file.toUri().getPath().toString(),e);
            return null;
        } catch (InstantiationException e) {
            log.error("Error reading first row from: "+file.toUri().getPath().toString(),e);
            return null;
        }
    }


    // Fields

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public Path[] getAllChildrenFiles(Path inputPath) throws IOException {
        if(fs.isFile(inputPath)) {
            Path[] tmp = new Path[1];
            tmp[0] = inputPath;
            return tmp;
        } else {
            FileStatus[] children = fs.listStatus(inputPath);
            ArrayList<Path> childrenPaths = new ArrayList<Path>(children.length);
            for(FileStatus child : children) {
                if(fs.isFile(child.getPath())) {
                    childrenPaths.add(child.getPath());
                }
            }

            return childrenPaths.toArray(new Path[childrenPaths.size()]);
        }
    }

    public String composeInputPathString(Path input) throws IOException {
        log.info("Composing path for "+input.toUri().getPath());
        StringBuilder stringBuilder = new StringBuilder();
            boolean initial = true;
            for(Path child : getAllChildrenFiles(input)) {
                log.info("Adding path "+child.toUri().getPath());
                if(initial) {
                    stringBuilder.append(child.toUri().getPath());
                    initial = false;
                } else {
                    stringBuilder.append(","+child.toUri().getPath());
                }
            }

        return stringBuilder.toString();
    }

    // executes this task
    public abstract void run();

    // returns the path to the file where the output of this job is stored
    public abstract Path getOutputFile();

    // returns an object with the results of this job
    public abstract Object getOutput();

    // cleans _SUCCESS files
    public void cleanOutput() throws IOException {
        Path tmp = getOutputFile().suffix("/_SUCCESS");
        log.info("TRYING TO DELETE "+tmp.toUri().getPath());
        if(getFs().exists(tmp)) {
            log.info("DELETED!");
            getFs().delete(tmp,true);
        }
    }

    public void cleanOutput(Path output) throws IOException {
        Path tmp = output.suffix("/_SUCCESS");
        log.info("TRYING TO DELETE "+tmp.toUri().getPath());
        if(getFs().exists(tmp)) {
            log.info("DELETED!");
            getFs().delete(tmp,true);
        }
    }
}
