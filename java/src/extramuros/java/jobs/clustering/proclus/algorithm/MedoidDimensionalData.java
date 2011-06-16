package extramuros.java.jobs.clustering.proclus.algorithm;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 10:23
 */
public class MedoidDimensionalData implements Writable {

    // Medoid whose dimensional data is stored in this object
    private Medoid medoid;

    // Array of information associated to this medoid stored as double values
    private double[] data;

    public MedoidDimensionalData() {
        setMedoid(null);
        setData(new double[0]);
    }

    public MedoidDimensionalData(Medoid medoid) {
        this.setMedoid(medoid);
        setData(new double[medoid.getVector().getNumNondefaultElements()]);
    }

    public void add(double[] toAdd) {
        for(int i=0; i<data.length; i++) {
            data[i] = data[i] + toAdd[i];
        }
    }

    public void average(double total) {
        for(int i=0; i<data.length; i++) {
            data[i] = data[i] / total;
        }
    }

    public void write(DataOutput out) throws IOException {
        medoid.write(out);
        out.writeInt(getData().length);
        for(double datum : data) {
            out.writeDouble(datum);
        }
    }

    public void readFields(DataInput in) throws IOException {
        medoid = new Medoid();
        medoid.readFields(in);

        int dataSize = in.readInt();

        data = new double[dataSize];
        for(int i=0; i<dataSize; i++) {
            data[i] = in.readDouble();
        }
    }

    // Accessors

    public Medoid getMedoid() {
        return medoid;
    }

    public void setMedoid(Medoid medoid) {
        this.medoid = medoid;
    }

    public double[] getData() {
        return data;
    }

    public void setData(double[] data) {
        this.data = data;
    }
}
