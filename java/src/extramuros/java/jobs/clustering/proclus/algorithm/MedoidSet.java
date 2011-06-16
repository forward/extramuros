package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * User: antonio
 * Date: 24/05/2011
 * Time: 10:02
 */
public class MedoidSet implements Writable, WritableComparable<MedoidSet> {

    private static final Logger log = LoggerFactory.getLogger(MedoidSet.class);

    // medoids in the set
    private ArrayList<Medoid> medoids;

    public MedoidSet() {
        this.medoids = new ArrayList<Medoid>();
    }

    public MedoidSet(Medoid[] medoids) {
        this.medoids = new ArrayList<Medoid>(medoids.length);
        for(int i=0; i<medoids.length; i++) {
            Medoid medoid = medoids[i];
            medoid.setLabel(""+i);
            this.medoids.add(medoid);
        }
    }

    public int count(){
        return medoids.size();
    }

    public void addMedoid(Medoid medoid) {
        medoid.setLabel(""+medoids.size());
        medoids.add(medoid);
    }

    public Medoid[] getMedoids() {
        return medoids.toArray(new Medoid[medoids.size()]);
    }

    public int compareTo(MedoidSet medoidSet) {
        if(this.count() == medoidSet.count()) {
            return 0;
        } else {
            if(this.count() < medoidSet.count()) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(medoids.size());
        for(Medoid medoid : medoids) {
            medoid.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        int numMedoids = in.readInt();
        medoids = new ArrayList<Medoid>();

        for(int i=0; i<numMedoids; i++) {
            Medoid medoid = new Medoid();
            medoid.readFields(in);
            medoids.add(medoid);
        }
    }

    public double minDist(Vector v) {
        double distance = 0;
        boolean initialized = false;

        for(Medoid medoid : medoids) {
            double newDistance = medoid.segmentalDistanceTo(v);
            if(newDistance<distance || !initialized) {
                distance = newDistance;
                initialized = true;
            }
        }

        return distance;
    }

    /*
     * forall medoid_i in Set -> delta_i = min(dist(medoid_i,medoid_j)), forall medoid_j != medoid_i
     */
    public void computeDeltas() {
        for(Medoid medoid_i : medoids) {
            double dist = 0;
            boolean initialized = false;
            for(Medoid medoid_j : medoids) {
                if(medoid_i.getLabel().compareTo(medoid_j.getLabel())!=0) {
                    double this_dist = medoid_i.distanceTo(medoid_j.getVector());
                    if(initialized==false || this_dist<dist) {
                        dist = this_dist;
                        initialized = true;
                    }
                }
            }
            medoid_i.setDelta(dist);
        }
    }

    public void computeDimensions(int l) {
        int numDimensions = count() * (l - 2);
        ArrayList<MedoidDimension> acum = new ArrayList<MedoidDimension>(count()*medoids.get(0).getVector().getNumNondefaultElements());

        for(int i=0; i<count(); i++){
            Medoid medoid = medoids.get(i);
            medoid.clearDimensions();

            MedoidDimension[] dimensions = medoid.reifyDimensions();
            Arrays.sort(dimensions);

            // constraint each medoid has at least 2 dimensions
            medoid.addDimension(dimensions[0].getIndex());
            medoid.addDimension(dimensions[1].getIndex());

            // acum the rest of dimensions
            for(int j=2; j<dimensions.length; j++) {
                acum.add(dimensions[j]);
            }
        }

        // sort in increasing order
        Collections.sort(acum);

        // add the dimensions
        for(int i=0; i<numDimensions; i++) {
            MedoidDimension dimension = acum.get(i);
            dimension.getMedoid().addDimension(dimension.getIndex());
        }
    }
}
