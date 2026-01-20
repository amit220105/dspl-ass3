package hadoop.examples;

import java.io.*;
import java.net.URI;
import java.util.*;

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
            File f = new File(uri.getPath());
            String name = f.getName().toLowerCase(Locale.ROOT);
            String label = name.contains("negative") ? "NEG"
                         : name.contains("positive") ? "POS"
                         : "UNK";

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