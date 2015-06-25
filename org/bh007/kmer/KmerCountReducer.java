package org.bh007.kmer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

public class KmerCountReducer extends Reducer<Text, IntWritable, IntWritable, IntWritable> {

    private IntWritable outputKey = new IntWritable();
    private IntWritable outputValue = new IntWritable();
    private MultipleOutputs<IntWritable, IntWritable> mos;
    private String baseOutputFolder = "";

    private int Size_Kmer;
    private int Size_Segment;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get( conf );
        mos = new MultipleOutputs<IntWritable,IntWritable>(context);

        Size_Kmer = Integer.parseInt( conf.get("Size.Kmer") );
        Size_Segment = Integer.parseInt( conf.get("Size.Segment") );

        Path newFolderPath = Path.mergePaths( fs.getWorkingDirectory(), new Path("/cache"));
        //if( fs.exists( newFolderPath ) )
        //    fs.delete( newFolderPath, true );
        baseOutputFolder = newFolderPath.toString();
    }

    @Override
    public void reduce( Text key, Iterable<IntWritable> value, Context context )
            throws IOException, InterruptedException {

        String[] fields = key.toString().split("\\:");
        int domain = Integer.parseInt( fields[0].trim() );
        int outkey = Integer.parseInt( fields[1].trim() );

        int sum = 0;
        for (IntWritable v : value)
            sum += v.get();
        outputKey.set( outkey );
        outputValue.set( sum );

        if ( domain == 0) {
            mos.write(outputKey, outputValue, baseOutputFolder + "/KmerTotal/kt");
        }
        else if ( domain == Size_Kmer) {
            mos.write( outputKey, outputValue, baseOutputFolder + "/K-mer/k"  );
            context.getCounter( Kmer.MyCounters.NKMER ).increment( 1 );
        }
        else if ( domain == Size_Kmer-1) {
            mos.write( outputKey, outputValue, baseOutputFolder + "/K-1mer/k1"  );
            context.getCounter( Kmer.MyCounters.NK_1MER ).increment(1);
        }
        else if ( domain == Size_Kmer-2) {
            mos.write( outputKey, outputValue, baseOutputFolder + "/K-2mer/k2"  );
            context.getCounter( Kmer.MyCounters.NK_2MER ).increment(1);
        }
    }

    public void cleanup( Context context )
            throws IOException, InterruptedException {

        mos.close();
    }
}