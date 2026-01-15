package hadoop.packages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BiarcsParser {

    public static class Tok {
        public final int id;
        public final String word;
        public final String pos;
        public final String rel;
        public final int head;
        
        public Tok(int id, String word, String pos, String rel, int head) {
            this.id = id;
            this.word = word;
            this.pos = pos;
            this.rel = rel;
            this.head = head;
        }
    }

    public static class Record {
        public final List<Tok> toks;
        public final long count;
        
        public Record(List<Tok> toks, long count) {
            this.toks = toks;
            this.count = count;
        }
    }

public static Record parseLine(String line) {
    line = line.trim();
    if (line.isEmpty()) return null;
    // head_word \t syntactic-ngram \t total_count \t ...
    String[] parts = line.split("\t", 4);
    if (parts.length < 3) {
        throw new IllegalArgumentException("Expected at least 3 tab-separated fields: " + line);
    }
    String ngram = parts[1];
    long totalCount;
    try {
        totalCount = Long.parseLong(parts[2]);
    } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Third field is not a count: " + parts[2]);
    }
    String[] tokens = ngram.split("\\s+");
    List<Tok> toks = new ArrayList<>();
    for (int i = 0; i < tokens.length; i++) {
        String[] fields = tokens[i].split("/", 4);
        if (fields.length != 4) {
            throw new IllegalArgumentException("Expected word/POS/dep/head format: " + tokens[i]);
        }
        String word = fields[0];
        String pos  = fields[1];
        String rel  = fields[2];
        int head;
        try {
            head = Integer.parseInt(fields[3]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid head index: " + fields[3]);
        }

        toks.add(new Tok(i + 1, word, pos, rel, head));
    }
    return new Record(toks, totalCount);
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