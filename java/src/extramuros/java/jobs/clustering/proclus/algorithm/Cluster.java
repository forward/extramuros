package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.clustering.Model;
import org.apache.mahout.common.parameters.Parameter;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;


/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 13:16
 */
public class Cluster implements Writable, WritableComparable<Cluster>, org.apache.mahout.clustering.Cluster {

    private Medoid medoid;
    private Vector centroid;
    private long numPoints;
    private boolean bestCluster;
    private ArrayList<Vector> observing;

    public Cluster() {
        setMedoid(null);
        setCentroid(null);
        setNumPoints(0);
        setBestCluster(true);
        observing = new ArrayList<Vector>();
    }

    public Cluster(Medoid medoid) {
        setMedoid(medoid);
        setCentroid(null);
        setNumPoints(0);
        setBestCluster(true);
        observing = new ArrayList<Vector>();
    }

    public String getLabel() {
        return getMedoid().getLabel();
    }

    public double[] segmentalDistancesToCentroid(Vector vector) {
        double[] dimensions = medoid.elementsInDimensions(vector);
        double[] medoidDimensions = medoid.elementsInDimensions();

        for(int i=0; i<dimensions.length; i++) {
            dimensions[i] = Math.abs(medoidDimensions[i] - dimensions[i]);
        }

        return dimensions;
    }

    // writable, comparable

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(getMedoid() !=null);
        if(getMedoid() !=null) {
            getMedoid().write(out);
        }

        out.writeBoolean(getCentroid() != null);
        if(getCentroid() !=null) {
            VectorWritable writable = new VectorWritable(getCentroid());
            writable.write(out);
        }

        out.writeLong(getNumPoints());

        out.writeBoolean(isBestCluster());

        out.writeInt(observing.size());
        for(Vector v : observing) {
            new VectorWritable(v).write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        boolean hasElem = in.readBoolean();
        if(hasElem) {
            setMedoid(new Medoid());
            getMedoid().readFields(in);
        } else {
            setMedoid(null);
        }

        hasElem = in.readBoolean();
        if(hasElem) {
            VectorWritable writable = new VectorWritable();
            writable.readFields(in);
            setCentroid(writable.get());
        } else {
            setCentroid(null);
        }

        setNumPoints(in.readLong());

        setBestCluster(in.readBoolean());

        int observingCount = in.readInt();
        observing = new ArrayList<Vector>(observingCount);
        for(int i=0; i<observingCount; i++) {
            VectorWritable writable = new VectorWritable();
            writable.readFields(in);
            observing.add(writable.get());
        }
    }

    public int compareTo(Cluster cluster) {
        return cluster.getMedoid().getLabel().compareTo(getMedoid().getLabel());
    }


    // accessors

    public Medoid getMedoid() {
        return medoid;
    }

    public void setMedoid(Medoid medoid) {
        this.medoid = medoid;
    }

    public Vector getCentroid() {
        return centroid;
    }

    public void setCentroid(Vector centroid) {
        this.centroid = centroid;
    }

    // Cluster interface

    public int getId() {
        String[] parts = getLabel().split("-");
        return Integer.parseInt(parts[parts.length-1]);
    }

    public Vector getCenter() {
        return getMedoid().getVector();
    }

    public Vector getRadius() {
        DenseVector radius = new DenseVector(getMedoid().getVector().getNumNondefaultElements());
        int[] dimensions = getMedoid().getDimensions();
        double sumDims = 0;
        for(int dim : dimensions) {
            sumDims = sumDims + getMedoid().getVector().get(dim);
        }

        Arrays.sort(dimensions);
        int dimensionsCounter = 0;
        for(int i=0; i<getMedoid().getVector().getNumNondefaultElements(); i++) {
            if(dimensionsCounter<dimensions.length && i == dimensions[dimensionsCounter]) {
                double dim = getMedoid().getVector().get(dimensions[dimensionsCounter]);
                double percentage = dim / sumDims;
                radius.set(i,percentage * getMedoid().getDelta());
                dimensionsCounter++;
            } else {
                radius.set(i,0);
            }
        }

        return radius;
    }

    public long getNumPoints() {
        return numPoints;
    }

    public String asFormatString(String[] bindings) {
        return toString();
    }

    public void setNumPoints(int numPoints) {
        this.numPoints = (int) numPoints;
    }

    public void setNumPoints(long numPoints) {
        this.numPoints = numPoints;
    }

    public boolean isBestCluster() {
        return bestCluster;
    }

    public void setBestCluster(boolean bestCluster) {
        this.bestCluster = bestCluster;
    }

    public double pdf(VectorWritable x) {
        return 0;
    }

    public void observe(VectorWritable x) {
        observing.add(x.get());
    }

    public void observe(VectorWritable vectorWritable, double v) {
    }

    public void computeParameters() {
    }

    public long count() {
        return numPoints;
    }

    public Model<VectorWritable> sampleFromPosterior() {
        return null;
    }

    public Collection<Parameter<?>> getParameters() {
        return new ArrayList<Parameter<?>>();
    }

    public void createParameters(String prefix, Configuration jobConf) {
    }

    public void configure(Configuration config) {
    }
}
