package hadoop.packages;

import java.util.*;

public class BiarcsParser {

    public static class Tok {
        public final int id;        // 1..N within the record
        public final String word;   // surface word
        public final String pos;    // POS tag
        public final String rel;    // dependency relation label
        public final int head;      // 0..N (0 = ROOT)
        public Tok(int id, String word, String pos, String rel, int head) {
            this.id = id; this.word = word; this.pos = pos; this.rel = rel; this.head = head;
        }
        @Override public String toString() {
            return id + ":" + word + "/" + pos + "/" + rel + "/" + head;
        }
    }

    public static class Record {
        public final String lemma;      // first column 
        public final List<Tok> toks;    // groups
        public final long count;        // last column
        public Record(String lemma, List<Tok> toks, long count) {
            this.lemma = lemma; this.toks = toks; this.count = count;
        }
    }

    public static Record parseLine(String line) {
        line = line.trim();
        if (line.isEmpty()) return null;

        // split by whitespace; format: lemma + groups + count
        String[] parts = line.split("\\s+");
        if (parts.length < 3) {
            throw new IllegalArgumentException("Bad biarcs line (need lemma + >=1 group + count): " + line);
        }

        String lemma = parts[0];
        long count;
        try {
            count = Long.parseLong(parts[parts.length - 1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Last field is not a count: " + parts[parts.length - 1] + " in line: " + line);
        }

        List<Tok> toks = new ArrayList<>();
        int id = 1;
        for (int i = 1; i < parts.length - 1; i++) {
            String g = parts[i];
            // group format: word/POS/rel/headIndex
            // use split limit=4 to avoid accidental extra splits
            String[] gp = g.split("/", 4);
            if (gp.length != 4) {
                throw new IllegalArgumentException("Bad group (expected word/POS/rel/head): " + g + " in line: " + line);
            }
            String word = gp[0];
            String pos = gp[1];
            String rel = gp[2];

            int head;
            try {
                head = Integer.parseInt(gp[3]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Bad head index in group: " + g + " line: " + line);
            }

            toks.add(new Tok(id, word, pos, rel, head));
            id++;
        }

        // sanity: head index must be 0..N
        int n = toks.size();
        for (Tok t : toks) {
            if (t.head < 0 || t.head > n) {
                throw new IllegalArgumentException("Head out of range: " + t + " (N=" + n + ") line: " + line);
            }
        }

        return new Record(lemma, toks, count);
    }

    public static Map<Integer, List<Integer>> buildUndirectedAdj(List<Tok> toks) {
        Map<Integer, List<Integer>> adj = new HashMap<>();
        for (Tok t : toks) {
            adj.computeIfAbsent(t.id, k -> new ArrayList<>());
            if (t.head != 0) {
                adj.computeIfAbsent(t.head, k -> new ArrayList<>());
                adj.get(t.id).add(t.head);
                adj.get(t.head).add(t.id);
            }
        }
        return adj;
    }
}
