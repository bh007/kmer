package org.bh007.kmer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CVReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
    }

    @Override
    public void reduce( IntWritable key, Iterable<DoubleWritable> value, Context context )
            throws IOException, InterruptedException {
        double outvalue = 0.;
        for (DoubleWritable v : value)
            outvalue = v.get();
        context.write( key, new DoubleWritable( outvalue ));

    }
}
