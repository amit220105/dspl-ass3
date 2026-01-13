package hadoop.packages;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class BiarcsExtractor {
    public static class Event {
        public final String predicateKey; // p
        public final String slot;         // "X" or "Y"
        public final String filler;       // w
        public final long count;          // record frequency

        public Event(String predicateKey, String slot, String filler, long count) {
            this.predicateKey = predicateKey;
            this.slot = slot;
            this.filler = filler;
            this.count = count;
        }
    }
    public List<Event> extract(BiarcsParser.Record r) {
        if (r == null) return Collections.emptyList();
        BiarcsParser.Tok [] byId = new BiarcsParser.Tok[r.toks.size() + 1];
        // index tokens by their ID for easy access
        for (BiarcsParser.Tok t : r.toks) byId[t.id] = t;

        Map<Integer, List<Integer>> adj = BiarcsParser.buildUndirectedAdj(r.toks);
        List<BiarcsParser.Tok> nouns = new ArrayList<>();
        for (BiarcsParser.Tok t : r.toks) {
            if (isNoun(t.pos)) {
                nouns.add(t);
            }
        }
        if (nouns.size() < 2) return Collections.emptyList();

        List<Event> out = new ArrayList<>();

        for (int i = 0; i < nouns.size(); i++) {
            for (int j = i + 1; j < nouns.size(); j++) {
                BiarcsParser.Tok n1 = nouns.get(i);
                BiarcsParser.Tok n2 = nouns.get(j);

                List<Integer> path = shortestPath(adj, n1.id, n2.id);
                if (path == null) continue;

                Integer verbId = chooseVerbOnPath(byId, path);
                if (verbId == null) continue;

                String predicate = buildPredicateKey(byId, path, verbId);
                if (predicate == null) continue;

                // X/Y assignment: subject-of-verb is X if detectable
                BiarcsParser.Tok xTok = n1, yTok = n2;
                if (isSubjectOf(n2, verbId) && !isSubjectOf(n1, verbId)) {
                    xTok = n2; yTok = n1;
                }

                out.add(new Event(predicate, "X", normWord(xTok.word), r.count));
                out.add(new Event(predicate, "Y", normWord(yTok.word), r.count));
            }
        }

        return out;
    }

    // ---------------- helpers ----------------

    private static boolean isNoun(String pos) { return pos != null && pos.startsWith("NN"); }
    private static boolean isVerb(String pos) { return pos != null && pos.startsWith("VB"); }
    private static boolean isPrep(String pos) { return "IN".equals(pos) || "TO".equals(pos); }

    private static String normWord(String w) {
        return w.toLowerCase(Locale.ROOT);
    }

    private static boolean isSubjectOf(BiarcsParser.Tok n, int verbId) {
        return n.head == verbId && ("nsubj".equals(n.rel) || "nsubjpass".equals(n.rel));
    }

    private static Integer chooseVerbOnPath(BiarcsParser.Tok[] byId, List<Integer> path) {
        for (int id : path) {
            BiarcsParser.Tok t = byId[id];
            if (t != null && isVerb(t.pos)) return id;
        }
        return null;
    }

    private static String buildPredicateKey(BiarcsParser.Tok[] byId, List<Integer> path, int verbId) {
        BiarcsParser.Tok verb = byId[verbId];
        if (verb == null) return null;

        List<String> parts = new ArrayList<>();
        parts.add("X");
        parts.add(normWord(verb.word));

        // include IN/TO tokens along the path (excluding endpoints and the verb)
        for (int k = 1; k < path.size() - 1; k++) {
            int id = path.get(k);
            if (id == verbId) continue;
            BiarcsParser.Tok t = byId[id];
            if (t != null && isPrep(t.pos)) parts.add(normWord(t.word));
        }

        parts.add("Y");
        return String.join(" ", parts).replaceAll("\\s+", " ").trim();
    }

    private static List<Integer> shortestPath(Map<Integer, List<Integer>> adj, int start, int goal) {
        ArrayDeque<Integer> q = new ArrayDeque<>();
        Map<Integer, Integer> parent = new HashMap<>();
        q.add(start);
        parent.put(start, -1);

        while (!q.isEmpty()) {
            int cur = q.removeFirst();
            if (cur == goal) break;
            for (int nxt : adj.getOrDefault(cur, Collections.<Integer>emptyList())) {
                if (parent.containsKey(nxt)) continue;
                parent.put(nxt, cur);
                q.addLast(nxt);
            }
        }
        if (!parent.containsKey(goal)) return null;

        List<Integer> path = new ArrayList<>();
        for (int x = goal; x != -1; x = parent.get(x)) path.add(x);
        Collections.reverse(path);
        return path;
    }
}
