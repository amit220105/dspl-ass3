90% — Below are two ready-to-submit drafts (Design + Analysis) plus a concise “Article Understanding” write-up. I did not invent any experimental numbers; wherever results depend on your run outputs, I provided fill-in fields and the exact procedure/code to compute them.

1) Article understanding (DIRT): problem, why it’s hard, steps
1.1 Problem statement

We want to automatically discover lexico-syntactic inference rules such as:

X is author of Y ≈ X wrote Y

X caused Y ≈ Y is triggered by X

DIRT models each predicate template as a dependency path with two argument slots (X and Y). It then computes similarity between paths using an extended distributional hypothesis:

Two paths are similar if they connect similar sets of slot fillers (words/entities).

Your assignment adapts DIRT to the Google syntactic n-grams (biarcs) dataset and implements it efficiently in MapReduce, then evaluates similarity scores against a labeled test set.

1.2 Why it is hard (practical + modeling)

Huge feature space / sparsity

There are many distinct paths p and many distinct fillers w.

Most (p, slot, w) combinations are rare → sparse vectors.

Computational scale

Naively comparing every pair of paths is infeasible (quadratic in number of paths).

Even storing all features for all paths requires careful aggregation.

Parser / representation mismatch

The paper’s Minipar omits some function words; your data includes prepositions.

You must incorporate IN/TO so that paths like “X … in Y” are captured.

Noise

Fillers can include punctuation-like tokens (%) or very frequent fillers (pronouns).

Such fillers can inflate similarity unless filtered/weighted.

Morphology mismatch

Test predicates are baseform; corpus verbs are inflected.

Without stemming, “involve” and “involves” are treated as different paths → weaker counts and similarity.

1.3 Core DIRT steps (mapped to your implementation)
Step A — Extract path instances and argument fillers

From each parsed record (a dependency tree/biarc), extract:

a dependency path p whose head is a verb

two noun arguments filling slots X and Y

Each occurrence yields two feature events:

(p, SlotX, wX) and (p, SlotY, wY) with the record frequency count.

Step B — Build the triple database (counts)

Aggregate counts:

c(p, slot, w) = how often word w fills slot for path p

This is the “triple database” used by DIRT.

Step C — Convert counts to association weights (MI / PMI)

Compute:

PS(p,slot) = Σ_w c(p,slot,w) (how frequent the path-slot is)

SW(slot,w) = Σ_p c(p,slot,w) (how frequent the filler is in that slot)

S(slot) = Σ_{p,w} c(p,slot,w) (slot total)

Then compute mutual information / PMI:

MI(p,slot,w)=log⁡c(p,slot,w)⋅S(slot)PS(p,slot)⋅SW(slot,w)
MI(p,slot,w)=log
PS(p,slot)⋅SW(slot,w)
c(p,slot,w)⋅S(slot)
	​


Commonly you keep only positive MI (PMI+): drop MI <= 0.

Step D — Compute similarity between paths for the test set

For a given pair of paths (p1, p2):

Compute similarity for SlotX: compare vectors {(w, MI(p1,X,w))} vs {(w, MI(p2,X,w))}

Compute similarity for SlotY similarly.

Combine using geometric mean (paper):

Sim(p1,p2)=SimX⋅SimY
Sim(p1,p2)=
SimX⋅SimY
​


In practice, you implement this by joining p1 and p2 features on the common filler words w, plus maintaining slot-wise normalizers.

Step E — Evaluate

Given labeled test pairs (p1, p2, label), compute:

similarity score per pair

choose threshold(s) to classify entailment

compute F1 and precision-recall curves

do error analysis (TP/FP/TN/FN examples)

2) System design report (ready-to-submit draft)
2.1 High-level architecture (pipeline)

Goal outputs (as required):

Test-set similarities: path1 \t path2 \t sim \t label

MI table: p \t slot \t w \t MI(p,slot,w) for every observed triple

Pipeline overview (logical)

Job1: Extract & count triples c(p,slot,w)

Job2A: Compute marginals PS, SW, S

Job2B: Compute MI values from Job1 + Job2A

Job3A: Compute per-(p,slot) normalizers (e.g., sum of MI)

Job3B3: Compute slot similarity for each test pair (X and Y separately)

Job3B4: Combine X/Y similarities into final similarity; join label; output

2.2 Component/class responsibilities (table)
A) Parsing & extraction utilities (non-MapReduce)
Class	Responsibility	Why it matters	Input → Output
BiarcsParser	Parses one biarc record line into tokens with fields (word,pos,rel,head) and record frequency count. Builds adjacency graph.	Correct parsing is prerequisite; any error here breaks all counts.	Raw line → Record(toks[], count)
BiarcsExtractor	From a parsed record, selects noun pairs, finds shortest dependency path, chooses a verb head, builds predicate string p, assigns X/Y roles, emits feature events (p,slot,w,count) (two per noun pair).	This defines what a “path” means in your system; must enforce constraints (verb head, noun slots, include IN/TO, stem verbs, filter auxiliaries).	Record → List<Event(p,slot,w,count)>
Stemmer	Porter stemming for verb normalization (used in predicate construction).	Required to match test-set baseforms and reduce sparsity.	token → stemmed token
B) MapReduce jobs (what each class does)
Job1 — Job1TripleCounts

Purpose: Build triple database counts c(p,slot,w).

Item	Definition
Input	Biarcs dataset lines
Mapper output key	p \t slot \t w
Mapper output value	count (record frequency)
Combiner/Reducer	Sum counts
Output	p \t slot \t w \t c(p,slot,w)

Importance: This is the foundational dataset used for MI and similarity.

Job2A — (your Job2A class; referenced by configuration)

Purpose: Compute marginals needed for MI.

Marginal	Meaning	Output record (suggested)
PS(p,slot)	total for each path-slot	PS \t p \t slot \t ps
SW(slot,w)	total for each slot-word	SW \t slot \t w \t sw
S(slot)	total mass per slot	S \t slot \t s

Importance: MI requires these; doing it as a separate aggregation avoids repeated scanning.

Job2B — MI computation (two alternative implementations in your codebase)

You have two designs:

Option 1: Map-side join MI (single job) — Job2B_ComputeMI

Loads all Job2A outputs into memory in mapper setup:

psMap[(p,slot)]

swMap[(slot,w)]

sMap[slot]

Reads Job1 lines, looks up marginals, computes MI, emits MI.

Pros: single job, simple.
Cons (major): SW(slot,w) can be extremely large → can exceed mapper memory. Use only if small input or strong filtering.

Option 2: Reduce-side joins (scalable) — Job2B1_JoinPS + Job2B2_JoinSWComputeMI

Job2B1_JoinPS: join Job1 with PS on (p,slot)
Output: p \t slot \t w \t c \t ps

Job2B2_JoinSWComputeMI: join output with SW on (slot,w) and load S(slot) in memory (small: X/Y)
Output: p \t slot \t w \t MI

Importance: This produces the required deliverable: MI(p,slot,w) for all triples.

Job3A — (your “feature vectors” stage; referenced earlier)

Purpose: Compute per-(p,slot) normalizer needed for Lin-style similarity (commonly sum of MI).

Typical output:

p \t slot \t w \t MI \t sumMI(p,slot) or separate:

SUM \t p \t slot \t sumMI

Importance: The similarity denominator requires total weight per slot vector.

Job3B3 — Slot similarity for each test pair (X and Y)

Purpose: For each test pair (p1, p2) and slot s∈{X,Y}, compute:

numerator = sum of MI weights over shared fillers w

denominator = total MI weight of both vectors

slotSim = (2 * numerator) / (denom) (common Lin-style)

Typical output (as you referenced):

p1 \t p2 \t slot \t slotSim

Importance: Avoids computing similarities for all path pairs; limits to test set.

Job3B4 — Job3B4_TestSetSimilarity

Purpose: Combine SlotX and SlotY similarities into final score, attach label:

finalSim=slotSimX⋅slotSimY
finalSim=
slotSimX⋅slotSimY
​


If a slot is missing, treat its similarity as 0.

Output:

path1 \t path2 \t finalSim \t label

Importance: This is the core output used for evaluation (F1 and PR curve).

2.3 Data volume + memory estimates (what to write in design)

Use these as reportable estimates; replace with your actual counters/logs if you have them.

Job1

Emits: ~2 events per noun-pair per record; worst-case O(#nouns²) per record, but bounded by record size.

Key size: p (tens of chars) + slot (1) + w (up to ~20 chars) → ~100 bytes typical.

Value size: 8 bytes (long).

Reducer memory: streaming sum (low), but shuffle volume is high.

Job2A

Input: Job1 output

Emits:

one PS per distinct (p,slot)

one SW per distinct (slot,w)

two S(slot) totals

The dominating structure is SW(slot,w) (large).

Job2B map-side join (if used)

Mapper setup loads all SW(slot,w); this can be millions of entries.

Memory risk: HashMap overhead can be > 2–3× raw data size.

Job2B reduce-side join (recommended)

Job2B2 only loads S(slot) (2 keys) in memory.

All other joins are streaming in reducers.

Job3B3

Operates on test pairs only (bounded by test set size).

Good for budget constraints (small/large inputs).

2.4 Required deliverables checklist (design report section)

You must state you produce:

Similarities for all test pairs, including finalSim=0.0 pairs (do not drop them).

MI table p,slot,w,mi for all observed triples.

(For frontal check) feature vectors/normalizers used to compute similarity (e.g., sumMI).

3) “Write everything needed” (submission package contents)
3.1 What you will submit

Design report (use Section 2 as-is)

Analysis report (Section 4 template + filled results)

Code (all Java classes + build instructions)

Outputs:

test-set similarities: path1 path2 finalSim label

mi table: p slot w mi

optionally: sumMI tables / intermediate outputs for reproducibility

3.2 What to say about constraints (explicitly, in writing)

Include a short paragraph in the design:

We only keep paths with verb heads, filter auxiliaries.

Slots X/Y are noun fillers.

Prepositions IN/TO are included in predicate key construction.

Verb in predicate is stemmed using Porter stemmer.

Optional: filter punctuation fillers.

4) Analysis report (ready-to-submit draft + how to compute)
4.1 Experimental setup

Datasets

Small: 10 input files from Biarcs dataset

Large: 100 input files from Biarcs dataset (budget permitting)

Test set

Tab-separated file of (path1, path2, label) where label ∈ {0,1}

We compute similarity for every pair and evaluate against the labels.

Outputs used

job3b4Out/part-*: path1 \t path2 \t finalSim \t label

4.2 Metrics

For a chosen threshold t:

Predict 1 if finalSim >= t, else 0.

Compute:

Precision = TP/(TP+FP)

Recall = TP/(TP+FN)

F1 = 2PR/(P+R)

We report:

Best-F1 threshold on Small and on Large (chosen by scanning thresholds)

F1 at that threshold for both sizes

Precision–Recall curve for both sizes

4.3 Threshold selection method

Method used:

Collect all unique similarity scores from the output.

For each candidate threshold (unique score), compute precision/recall/F1.

Choose threshold that maximizes F1 (ties broken by higher recall, or report both).

4.4 Precision–Recall curve

Compute PR points by sweeping threshold from high to low.

4.5 Error analysis protocol

Using the chosen threshold t* per dataset:

Extract examples for:

True Positive (label=1, pred=1)

False Positive (label=0, pred=1)

True Negative (label=0, pred=0)

False Negative (label=1, pred=0)

For each category:

select 5 examples (preferably:

FP: highest similarity among label=0

FN: lowest similarity among label=1

TP: highest similarity label=1

TN: lowest similarity label=0)

Compare their scores between Small vs Large (same (p1,p2)).

Identify patterns:

antonyms/polarity flip (classic DIRT weakness)

high-frequency fillers dominating similarity

parsing/preposition path mismatch

sparsity (missing slot evidence → 0 similarity)

4.6 Results section (fill-in)

You will paste the computed values here:

Small (10 files)

Selected threshold t_small = ______

Precision = ______

Recall = ______

F1 = ______

Large (100 files)

Selected threshold t_large = ______

Precision = ______

Recall = ______

F1 = ______

Qualitative comparison

Similarity scores shift: (describe observed trend)

Coverage: number of pairs with finalSim>0 increased/decreased

4.7 Required plots

PR curve (Small)

PR curve (Large)

Include axes:

x = recall

y = precision

4.8 Exact computation script (run locally; edit file paths)

Paste this into a Python file (or notebook). It assumes the output is:
p1<TAB>p2<TAB>sim<TAB>label

import glob
import math
import pandas as pd
import matplotlib.pyplot as plt

def load_outputs(pattern):
    rows = []
    for fp in glob.glob(pattern):
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: 
                    continue
                parts = line.split("\t")
                if len(parts) != 4:
                    continue
                p1, p2, sim_s, lab_s = parts
                try:
                    sim = float(sim_s)
                    lab = int(lab_s)
                except:
                    continue
                rows.append((p1, p2, sim, lab))
    return pd.DataFrame(rows, columns=["p1","p2","sim","label"])

def metrics_at_threshold(df, t):
    pred = (df["sim"] >= t).astype(int)
    lab = df["label"]

    tp = int(((pred==1) & (lab==1)).sum())
    fp = int(((pred==1) & (lab==0)).sum())
    tn = int(((pred==0) & (lab==0)).sum())
    fn = int(((pred==0) & (lab==1)).sum())

    precision = tp / (tp+fp) if (tp+fp) else 0.0
    recall    = tp / (tp+fn) if (tp+fn) else 0.0
    f1 = (2*precision*recall)/(precision+recall) if (precision+recall) else 0.0
    return precision, recall, f1, tp, fp, tn, fn

def best_f1(df):
    # include 0.0 as threshold candidate
    thresholds = sorted(set(df["sim"].tolist()))
    best = (-1.0, None, None)
    for t in thresholds:
        p,r,f1, *_ = metrics_at_threshold(df, t)
        if f1 > best[0]:
            best = (f1, t, (p,r))
    return best  # (best_f1, threshold, (precision, recall))

def pr_curve(df):
    # Sweep high -> low
    thresholds = sorted(set(df["sim"].tolist()), reverse=True)
    pts = []
    for t in thresholds:
        p,r,f1, *_ = metrics_at_threshold(df, t)
        pts.append((r,p,t,f1))
    return pts

def plot_pr(pts, title):
    recall = [x[0] for x in pts]
    prec   = [x[1] for x in pts]
    plt.figure()
    plt.plot(recall, prec)
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title(title)
    plt.grid(True)
    plt.show()

def top_examples(df, t, category, k=5):
    pred = (df["sim"] >= t).astype(int)
    lab  = df["label"]
    df2 = df.copy()
    df2["pred"] = pred

    if category == "TP":
        sub = df2[(df2.pred==1) & (df2.label==1)].sort_values("sim", ascending=False)
    elif category == "FP":
        sub = df2[(df2.pred==1) & (df2.label==0)].sort_values("sim", ascending=False)
    elif category == "TN":
        sub = df2[(df2.pred==0) & (df2.label==0)].sort_values("sim", ascending=True)
    elif category == "FN":
        sub = df2[(df2.pred==0) & (df2.label==1)].sort_values("sim", ascending=True)
    else:
        raise ValueError("category must be TP/FP/TN/FN")
    return sub.head(k)[["p1","p2","sim","label","pred"]]

# --- Example usage ---
# Small:
# df_small = load_outputs("/path/to/small/job3b4Out/part-*")
# f1_s, t_s, (p_s, r_s) = best_f1(df_small)
# pts_small = pr_curve(df_small)
# plot_pr(pts_small, f"PR Curve (Small)")

# Large:
# df_large = load_outputs("/path/to/large/job3b4Out/part-*")
# f1_l, t_l, (p_l, r_l) = best_f1(df_large)
# pts_large = pr_curve(df_large)
# plot_pr(pts_large, f"PR Curve (Large)")

# Error analysis examples:
# print(top_examples(df_small, t_s, "TP"))
# print(top_examples(df_small, t_s, "FP"))
# print(top_examples(df_small, t_s, "TN"))
# print(top_examples(df_small, t_s, "FN"))

5) Final self-check (what graders will look for)

Use this as your “double check” list before submitting:

Outputs

Test-set similarity output contains both labels 0 and 1, and includes rows even when sim=0.0.

MI output exists: p slot w mi for all observed triples.

Design report

Each job has mapper/reducer key/value definitions.

Input/output paths and formats are stated.

Memory risk is addressed (especially map-side joining SW).

Constraints from the assignment are explicitly handled (verb head, noun slots, IN/TO, stemmer, auxiliary filter).

Analysis report

Small and Large runs are both reported (or Large is explicitly justified as not run).

Threshold selection method described.

F1 reported for both sizes.

PR curves for both sizes.

Error analysis: 5 TP/FP/TN/FN each, with small vs large score comparison and interpretation.

If you want this delivered as a Word/PDF submission pack (Design.docx + Analysis.docx), say so and I will generate the files in the required format.