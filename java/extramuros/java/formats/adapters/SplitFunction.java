package extramuros.java.formats.adapters;

/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 09:04
 */
public interface SplitFunction<L,P> {
    // Inputs a Line object and returns Parts for that Line
    public P[] split(L input);
}
