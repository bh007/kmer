package org.bh007.kmer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KmerCountPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        String[] fields = key.toString().split("\\:");
        int kmerSize = Integer.parseInt( fields[0].trim() );
        int index = Integer.parseInt( fields[1].trim() );
        int range, base;

        if ( kmerSize==0 || numReduceTasks<2 ) { return 0; }
        else {
			base = 1 << (2 * kmerSize);
            range = base / numReduceTasks;
			if ( base % numReduceTasks != 0 ) { range += 1; }
            return index / range;
        }
    }
}
