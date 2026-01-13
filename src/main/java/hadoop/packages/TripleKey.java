package hadoop.packages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class TripleKey implements WritableComparable<TripleKey> {
    private String predicate; // p
    private String slot;      // "X" or "Y"
    private String filler;    // w

    public TripleKey() {}

    public TripleKey(String predicate, String slot, String filler) {
        this.predicate = predicate;
        this.slot = slot;
        this.filler = filler;
    }

    public String getPredicate() { return predicate; }
    public String getSlot() { return slot; }
    public String getFiller() { return filler; }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, predicate);
        WritableUtils.writeString(out, slot);
        WritableUtils.writeString(out, filler);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        predicate = WritableUtils.readString(in);
        slot = WritableUtils.readString(in);
        filler = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(TripleKey other) {
        int c = this.predicate.compareTo(other.predicate);
        if (c != 0) return c;
        c = this.slot.compareTo(other.slot);
        if (c != 0) return c;
        return this.filler.compareTo(other.filler);
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + predicate.hashCode();
        h = 31 * h + slot.hashCode();
        h = 31 * h + filler.hashCode();
        return h;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TripleKey)) return false;
        TripleKey other = (TripleKey) o;
        return predicate.equals(other.predicate)
                && slot.equals(other.slot)
                && filler.equals(other.filler);
    }

    @Override
    public String toString() {
        return predicate + "\t" + slot + "\t" + filler;
    }
    
}
