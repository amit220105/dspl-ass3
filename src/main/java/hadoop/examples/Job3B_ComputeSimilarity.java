package hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Job3B: Compute DIRT path similarity using Lin's formula.
 * 
 * Input: Job3A output (p \t slot \t w \t MI \t slotSum)
 * Output: path1 \t path2 \t similarity_score
 */
public class Job3B_ComputeSimilarity {

    // Feature: (word, MI, slotSum)
    static class Feature {
        String word;
        double mi;
        double slotSum;

        Feature(String word, double mi, double slotSum) {
            this.word = word;
            this.mi = mi;
            this.slotSum = slotSum;
        }
    }

    // PathVector: features for SlotX and SlotY
    static class PathVector {
        String path;
        Map<String, Feature> slotX = new HashMap<>();
        Map<String, Feature> slotY = new HashMap<>();
        double sumMI_X = 0.0;
        double sumMI_Y = 0.0;

        PathVector(String path) {
            this.path = path;
        }

        void addFeature(String slot, String word, double mi, double slotSum) {
            Feature f = new Feature(word, mi, slotSum);
            if ("X".equals(slot)) {
                slotX.put(word, f);
                sumMI_X = slotSum;
            } else if ("Y".equals(slot)) {
                slotY.put(word, f);
                sumMI_Y = slotSum;
            }
        }

        int featureCount() {
            return slotX.size() + slotY.size();
        }
    }

    // Similarity result
    static class SimilarityResult implements Comparable<SimilarityResult> {
        String path1;
        String path2;
        double similarity;

        SimilarityResult(String path1, String path2, double similarity) {
            this.path1 = path1;
            this.path2 = path2;
            this.similarity = similarity;
        }

        @Override
        public int compareTo(SimilarityResult other) {
            return Double.compare(this.similarity, other.similarity); // smallest first
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<String, PathVector> pathVectors = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outValue = new Text();
        
        private double minOverlapPercent = 0.01; // 1% minimum overlap
        private double minSimilarity = 0.0;      // minimum similarity threshold
        private int topK = 40;                    // top-K similar paths per path

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String job3APathStr = conf.get("job3AOutputPath");
            if (job3APathStr == null) {
                throw new IOException("Job3A output path not set in configuration");
            }

            minOverlapPercent = conf.getDouble("minOverlapPercent", 0.01);
            minSimilarity = conf.getDouble("minSimilarity", 0.0);
            topK = conf.getInt("topK", 40);

            System.err.println("Job3B Config: minOverlap=" + minOverlapPercent + 
                             ", minSim=" + minSimilarity + ", topK=" + topK);

            // Load all path vectors from Job3A output
            Path job3APath = new Path(job3APathStr);
            FileSystem fs = FileSystem.get(job3APath.toUri(), conf);
            
            int filesRead = 0;
            int linesRead = 0;
            
            for (FileStatus status : fs.listStatus(job3APath)) {
                if (!status.isFile()) continue;
                String fileName = status.getPath().getName();
                if (fileName.startsWith("_") || fileName.startsWith(".")) continue;
                if (!fileName.startsWith("part-")) continue;

                filesRead++;
                try (FSDataInputStream in = fs.open(status.getPath());
                     BufferedReader br = new BufferedReader(new InputStreamReader(in))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        linesRead++;

                        String[] parts = line.split("\t");
                        if (parts.length != 5) continue;

                        // Job3A output: p \t slot \t w \t MI \t slotSum
                        String path = parts[0];
                        String slot = parts[1];
                        String word = parts[2];
                        double mi;
                        double slotSum;
                        
                        try {
                            mi = Double.parseDouble(parts[3]);
                            slotSum = Double.parseDouble(parts[4]);
                        } catch (NumberFormatException e) {
                            continue;
                        }

                        PathVector pv = pathVectors.get(path);
                        if (pv == null) {
                            pv = new PathVector(path);
                            pathVectors.put(path, pv);
                        }
                        pv.addFeature(slot, word, mi, slotSum);
                    }
                }
            }

            System.err.println("Job3B loaded " + pathVectors.size() + " paths from " + 
                             filesRead + " files (" + linesRead + " lines)");
            
            if (pathVectors.isEmpty()) {
                throw new IOException("Failed to load path vectors from Job3A output");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 5) return;

            String path1 = parts[0];
            PathVector pv1 = pathVectors.get(path1);
            if (pv1 == null) return;

            // Find candidate paths (share at least one feature)
            Set<String> candidates = new HashSet<>();
            
            // Words in SlotX of path1
            for (String word : pv1.slotX.keySet()) {
                for (PathVector pv : pathVectors.values()) {
                    if (pv.path.equals(path1)) continue;
                    if (pv.slotX.containsKey(word)) {
                        candidates.add(pv.path);
                    }
                }
            }
            
            // Words in SlotY of path1
            for (String word : pv1.slotY.keySet()) {
                for (PathVector pv : pathVectors.values()) {
                    if (pv.path.equals(path1)) continue;
                    if (pv.slotY.containsKey(word)) {
                        candidates.add(pv.path);
                    }
                }
            }

            // Compute similarity for each candidate
            PriorityQueue<SimilarityResult> topResults = new PriorityQueue<>();
            
            for (String path2 : candidates) {
                PathVector pv2 = pathVectors.get(path2);
                if (pv2 == null) continue;

                // Filter by minimum overlap
                int commonFeatures = countCommonFeatures(pv1, pv2);
                int totalFeatures = pv1.featureCount() + pv2.featureCount();
                double overlapPercent = (double) commonFeatures / totalFeatures;
                
                if (overlapPercent < minOverlapPercent) continue;

                // Compute Lin similarity
                double sim = computeLinSimilarity(pv1, pv2);
                
                if (sim < minSimilarity) continue;

                topResults.add(new SimilarityResult(path1, path2, sim));
                if (topResults.size() > topK) {
                    topResults.poll(); // remove lowest
                }
            }

            // Emit top-K results
            List<SimilarityResult> results = new ArrayList<>();
            while (!topResults.isEmpty()) {
                results.add(0, topResults.poll()); // reverse order
            }

            for (SimilarityResult result : results) {
                outKey.set(result.path1);
                outValue.set(result.path2 + "\t" + result.similarity);
                context.write(outKey, outValue);
            }
        }

        private int countCommonFeatures(PathVector pv1, PathVector pv2) {
            int count = 0;
            for (String w : pv1.slotX.keySet()) {
                if (pv2.slotX.containsKey(w)) count++;
            }
            for (String w : pv1.slotY.keySet()) {
                if (pv2.slotY.containsKey(w)) count++;
            }
            return count;
        }

        /**
         * Compute Lin similarity using formula from DIRT paper:
         * sim(p1,p2) = sqrt( slotX_sim(p1,p2) * slotY_sim(p1,p2) )
         * 
         * where slotX_sim = sum(MI(p1,X,w) + MI(p2,X,w)) / (sumMI_X(p1) + sumMI_X(p2))
         *       for all w in common SlotX fillers
         */
        private double computeLinSimilarity(PathVector pv1, PathVector pv2) {
            // Compute SlotX similarity
            double slotX_numerator = 0.0;
            for (String w : pv1.slotX.keySet()) {
                if (pv2.slotX.containsKey(w)) {
                    slotX_numerator += pv1.slotX.get(w).mi + pv2.slotX.get(w).mi;
                }
            }
            double slotX_denominator = pv1.sumMI_X + pv2.sumMI_X;
            double slotX_sim = (slotX_denominator > 0) ? 
                slotX_numerator / slotX_denominator : 0.0;

            // Compute SlotY similarity
            double slotY_numerator = 0.0;
            for (String w : pv1.slotY.keySet()) {
                if (pv2.slotY.containsKey(w)) {
                    slotY_numerator += pv1.slotY.get(w).mi + pv2.slotY.get(w).mi;
                }
            }
            double slotY_denominator = pv1.sumMI_Y + pv2.sumMI_Y;
            double slotY_sim = (slotY_denominator > 0) ? 
                slotY_numerator / slotY_denominator : 0.0;

            // Geometric mean
            return Math.sqrt(slotX_sim * slotY_sim);
        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("Job3B_ComputeSimilarity args=" + java.util.Arrays.toString(args));

        int off = 0;
        if (args.length >= 4 && args[0].contains(".") && !args[0].startsWith("s3://")) {
            off = 1;
        }

        if (args.length < off + 3) {
            System.err.println("invalid input. args=" + java.util.Arrays.toString(args));
            System.err.println("Usage: Job3B <job3A-input> <job3A-output-path> <job3B-output> [minOverlap] [minSim] [topK]");
            System.exit(1);
        }

        String job3AIn  = args[off];       // re-read Job3A input for processing
        String job3AOut = args[off + 1];   // Job3A output path (for loading vectors)
        String job3BOut = args[off + 2];   // output for Job3B

        Configuration conf = new Configuration();
        conf.set("job3AOutputPath", job3AOut);
        
        // Optional parameters
        if (args.length > off + 3) {
            conf.setDouble("minOverlapPercent", Double.parseDouble(args[off + 3]));
        }
        if (args.length > off + 4) {
            conf.setDouble("minSimilarity", Double.parseDouble(args[off + 4]));
        }
        if (args.length > off + 5) {
            conf.setInt("topK", Integer.parseInt(args[off + 5]));
        }

        Job job = Job.getInstance(conf, "Job3B - Compute DIRT Path Similarity");
        job.setJarByClass(Job3B_ComputeSimilarity.class);

        job.setMapperClass(MapperClass.class);
        job.setNumReduceTasks(0); // map-only job

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input = Job3A output (re-read to process each path)
        FileInputFormat.addInputPath(job, new Path(job3AIn));

        // Output
        Path outPath = new Path(job3BOut);
        FileSystem outFs = FileSystem.get(outPath.toUri(), conf);
        if (outFs.exists(outPath)) {
            outFs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
