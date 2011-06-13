package extramuros.java.formats.adapters;

/**
 * User: antonio
 * Date: 03/06/2011
 * Time: 09:02
 */
public interface SkipFilter<L> {
    // Receives a Line and decides if it should be skipped
    public boolean skip(L line);
}
