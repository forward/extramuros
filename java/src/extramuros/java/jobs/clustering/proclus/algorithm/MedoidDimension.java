package extramuros.java.jobs.clustering.proclus.algorithm;

/**
 * User: antonio
 * Date: 25/05/2011
 * Time: 14:38
 */
public class MedoidDimension implements Comparable<MedoidDimension> {

    private Medoid medoid;
    private int index;
    private double weight;

    public MedoidDimension(Medoid medoid, int index, double weight){
        this.setMedoid(medoid);
        this.setIndex(index);
        this.setWeight(weight);
    }

    public int compareTo(MedoidDimension medoidDimension) {
        return (new Double(getWeight())).compareTo(new Double(medoidDimension.getWeight()));
    }

    public Medoid getMedoid() {
        return medoid;
    }

    public void setMedoid(Medoid medoid) {
        this.medoid = medoid;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
}
