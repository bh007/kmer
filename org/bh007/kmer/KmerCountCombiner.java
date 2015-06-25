package org.bh007.kmer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KmerCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException{

        int sum = 0;
        for (IntWritable v : value)
            sum += v.get();

        context.write( key, new IntWritable( sum ) );
    }
}
