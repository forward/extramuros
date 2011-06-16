package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: antonio
 * Date: 15/06/2011
 * Time: 10:23
 */
public class ProClusUtils {
    public static String composeInputPathString(FileSystem fs, Path input) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
            boolean initial = true;
            for(Path child : getAllChildrenFiles(fs, input)) {
                if(initial) {
                    stringBuilder.append(child.toUri().getPath());
                    initial = false;
                } else {
                    stringBuilder.append(","+child.toUri().getPath());
                }
            }

        return stringBuilder.toString();
    }

    public static Path[] getAllChildrenFiles(FileSystem fs, Path inputPath) throws IOException {
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

    public static void cleanOutput(FileSystem fs, Path output) throws IOException {
        Path tmp = output.suffix("/_SUCCESS");
        if(fs.exists(tmp)) {
            fs.delete(tmp,true);
        }
    }

}
