package hadoop.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TestPairsCache {

    // For fast filter: only emit contributions for pairs in test set
    public static final Map<String, Set<String>> partners = new HashMap<>();

    // Optional: keep labels POS/NEG for output
    public static final Map<String, String> pairLabel = new HashMap<>();

    public static void loadFromCacheFiles(URI[] cacheFiles) throws IOException {
    if (!partners.isEmpty()) return; // already loaded

    if (cacheFiles == null || cacheFiles.length == 0) {
        throw new IOException("No cache files provided for test set");
    }

    for (URI uri : cacheFiles) {
        // Prefer the localized filename provided via "#fragment"
        String localName = (uri.getFragment() != null && !uri.getFragment().isEmpty())
                ? uri.getFragment()
                : new org.apache.hadoop.fs.Path(uri.getPath()).getName();

        File f = new File(localName);

        // Determine label (prefer localName; fallback to path name)
        String nameForLabel = localName.toLowerCase(Locale.ROOT);
        if (nameForLabel.isEmpty()) {
            nameForLabel = new File(uri.getPath()).getName().toLowerCase(Locale.ROOT);
        }

        String label = nameForLabel.contains("negative") ? "NEG"
                     : nameForLabel.contains("positive") ? "POS"
                     : "UNK";

        if (!f.exists() || !f.isFile()) {
            // Helpful error: show what cache URIs we got and what local name we tried.
            throw new FileNotFoundException(
                    "DistributedCache file not found locally. " +
                    "Tried localName='" + localName + "' for cache URI='" + uri + "'. " +
                    "Make sure you add cache files with a fragment, e.g. s3://.../positive-preds.txt#positive-preds.txt");
        }

        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\t");
                if (parts.length < 2) continue;

                String a = PredicateNormalizer.normalizeTestTemplateToPredicateKey(parts[0]);
                String b = PredicateNormalizer.normalizeTestTemplateToPredicateKey(parts[1]);
                if (a == null || b == null) continue;

                addPair(a, b, label);
            }
        }
    }

    if (partners.isEmpty()) {
        throw new IOException("Loaded 0 test pairs from cache files");
    }
}

    private static void addPair(String a, String b, String label) {
        partners.computeIfAbsent(a, k -> new HashSet<>()).add(b);
        partners.computeIfAbsent(b, k -> new HashSet<>()).add(a);

        String p1 = (a.compareTo(b) <= 0) ? a : b;
        String p2 = (a.compareTo(b) <= 0) ? b : a;
        pairLabel.put(p1 + "\t" + p2, label);
    }

    public static boolean isTestPair(String a, String b) {
        Set<String> s = partners.get(a);
        return s != null && s.contains(b);
    }

    public static String labelFor(String a, String b) {
        String p1 = (a.compareTo(b) <= 0) ? a : b;
        String p2 = (a.compareTo(b) <= 0) ? b : a;
        return pairLabel.getOrDefault(p1 + "\t" + p2, "UNK");
    }
}