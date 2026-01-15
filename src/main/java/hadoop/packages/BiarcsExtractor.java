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

                String predicate = buildPredicateKeySimple(byId, path, verbId);
                if (predicate == null) continue;

                // X/Y assignment: subject-of-verb is X if detectable
                int x1 = roleScoreX(n1, byId, verbId);
                int x2 = roleScoreX(n2, byId, verbId);
                int y1 = roleScoreY(n1, byId, verbId);
                int y2 = roleScoreY(n2, byId, verbId);

                BiarcsParser.Tok xTok = n1, yTok = n2;

                // prefer clearer subject for X
                if (x2 > x1) {
                    xTok = n2; yTok = n1;
                } else if (x1 == x2) {
                    // if subject unclear, prefer clearer object for Y
                    if (y2 > y1) {
                        xTok = n1; yTok = n2;
                    } else if (y1 > y2) {
                        xTok = n2; yTok = n1;
                    } else {
                        // deterministic fallback: keep original order
                        xTok = n1; yTok = n2;
                    }
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
            if (t != null && isVerb(t.pos) && !isAuxVerb(t.word)) return id;
        }
        return null;

    }

    private static String stemLower(String w) {
    if (w == null || w.isEmpty()) return w;
    String lw = w.toLowerCase(Locale.ROOT);
    Stemmer s = new Stemmer();
    s.add(lw.toCharArray(), lw.length());
    s.stem();
    return s.toString();
    }   

    private static boolean isAuxVerb(String w) {
    if (w == null) return false;
    String x = w.toLowerCase(Locale.ROOT);
    return x.equals("be") || x.equals("is") || x.equals("are") || x.equals("was") || x.equals("were") ||
           x.equals("been") || x.equals("being") ||
           x.equals("have") || x.equals("has") || x.equals("had") ||
           x.equals("do") || x.equals("does") || x.equals("did");
}



private static String buildPredicateKey(BiarcsParser.Tok[] byId, List<Integer> path, int verbId) {
    if (path.size() < 2) return null;
    StringBuilder sb = new StringBuilder();
    sb.append("X");
    for (int i = 0; i < path.size() - 1; i++) {
        int aId = path.get(i);
        int bId = path.get(i + 1);
        BiarcsParser.Tok a = byId[aId];
        BiarcsParser.Tok b = byId[bId];
        if (a == null || b == null) return null;
        // decide direction and relation label
        String rel;
        String dir;
        if (a.head == b.id) {          // a -> head(b)
            rel = a.rel;
            dir = "^";                 // up
        } else if (b.head == a.id) {   // a is head of b
            rel = b.rel;
            dir = "v";                 // down
        } else {
            rel = "UNK";
            dir = "?";
        }
        sb.append(" --").append(rel).append(dir).append("-- ");
        // include node label for internal nodes (especially verb and preps)
        if (b.id == verbId) {
            sb.append("V(").append(stemLower(b.word)).append(")");
        } else if (isPrep(b.pos)) {
            sb.append("P(").append(normWord(b.word)).append(")");
        } else if (isVerb(b.pos)) {
            sb.append("V(").append(stemLower(b.word)).append(")");
        } else {
            sb.append(b.pos); // or omit for compactness
        }
    }
    sb.append(" Y");
    return sb.toString().replaceAll("\\s+", " ").trim();
}

private static String buildPredicateKeySimple(BiarcsParser.Tok[] byId, List<Integer> path, int verbId) {
    String verb = stemLower(byId[verbId].word); // you already stem => "provide" -> "provid"
    String prep = null;

    // pick the first IN/TO token on the path (typical for "provide from", "control with")
    for (int id : path) {
        BiarcsParser.Tok t = byId[id];
        if (t != null && ("IN".equals(t.pos) || "TO".equals(t.pos))) {
            prep = t.word.toLowerCase(java.util.Locale.ROOT);
            break;
        }
    }

    if (prep != null) return ("X " + verb + " " + prep + " Y").trim();
    return ("X " + verb + " Y").trim();
}



private static int roleScoreX(BiarcsParser.Tok n, BiarcsParser.Tok[] byId, int verbId) {
    // X = subject of verb
    if (n.head == verbId) {
        if ("nsubj".equals(n.rel) || "nsubjpass".equals(n.rel)) return 100;
    }
    return 0;
}
private static int roleScoreY(BiarcsParser.Tok n, BiarcsParser.Tok[] byId, int verbId) {
    // Y = object of verb
    if (n.head == verbId) {
        if ("dobj".equals(n.rel) || "obj".equals(n.rel) || "iobj".equals(n.rel)) return 100;
    }
    // Y = pobj of prep attached to verb
    if ("pobj".equals(n.rel)) {
        BiarcsParser.Tok prep = byId[n.head];
        if (prep != null && isPrep(prep.pos) && prep.head == verbId && "prep".equals(prep.rel)) return 80;
    }
    return 0;
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
