package org.bh007.kmer;

import java.io.*;
import org.apache.hadoop.io.*;

public class PairIntWritable implements WritableComparable<PairIntWritable> {
    private  IntWritable first;
    private  IntWritable second;

    public PairIntWritable() {
        set(new IntWritable(), new IntWritable());
    }

    public PairIntWritable(int f, int s) {
        set( new IntWritable(f), new IntWritable(s) );
    }

    public PairIntWritable(IntWritable first, IntWritable second) {
        set( first, second );
    }

    public void set(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public IntWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PairIntWritable) {
            PairIntWritable tp = (PairIntWritable) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first.toString() + ":" + second.toString();
    }

    @Override
    public int compareTo(PairIntWritable tp) {
        int cmp = first.compareTo(tp.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(tp.second);
    }
}

