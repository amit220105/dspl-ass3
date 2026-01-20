package hadoop.examples;


import java.util.Locale;

public class PredicateNormalizer {

    // Convert test template like "X confuse with Y" into the same predicateKey as extractor:
    // "X confus with Y" (stemmed verb), optional prep.
    public static String normalizeTestTemplateToPredicateKey(String t) {
        if (t == null) return null;
        t = t.trim().replaceAll("\\s+", " ");
        if (t.isEmpty()) return null;

        String[] toks = t.split(" ");
        if (toks.length < 3) return null;

        // Expect format: X VERB [PREP] Y  (or Y VERB [PREP] X)
        // Canonical output is always X ... Y
        String verb = toks[1];
        String stemVerb = stemLower(verb);

        String prep = null;
        if (toks.length >= 4) {
            prep = toks[2].toLowerCase(Locale.ROOT);
        }

        if (prep != null) return ("X " + stemVerb + " " + prep + " Y").trim();
        return ("X " + stemVerb + " Y").trim();
    }

    private static String stemLower(String w) {
        if (w == null || w.isEmpty()) return w;
        String lw = w.toLowerCase(Locale.ROOT);

        Stemmer s = new Stemmer();
        s.add(lw.toCharArray(), lw.length());
        s.stem();
        return s.toString();
    }
    
}
