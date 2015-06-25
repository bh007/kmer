package org.bh007.kmer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class KmerCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text kmer = new Text();
    private final static IntWritable one = new IntWritable( 1 );

    private int Size_Kmer;
    private int Size_Segment;

    //private String inputFile;
    //public void configure(JobConf job) {
    //	inputFile = job.get("map.input.file");
    //}
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        Size_Kmer = Integer.parseInt( conf.get("Size.Kmer") );
        Size_Segment = Integer.parseInt( conf.get("Size.Segment") );
    }

    @Override
    public void map( LongWritable key, Text value, Context context )
            throws IOException, InterruptedException {

        int[] kmerCounters = new int[]{0, 0, 0};

        String segment = value.toString().replace("\n", "");

        if (segment.charAt(0)=='-') {
            String candidate = "";
            int NPos;

            for (int i=1; i<=Size_Segment; i++) {
                if ( segment.charAt(i)=='N' ) continue;
                candidate = segment.substring(i, Size_Kmer + i);
                NPos = candidate.indexOf('N');
                if ( NPos<0 ) {
                    kmer.set(Integer.toString(Size_Kmer) + ":" + Utilities.kmerToIndex(candidate, 1));
                    context.write(kmer, one);
                    kmerCounters[0]++;
                }
                if ( NPos<0 || NPos>Size_Kmer-2 ) {
                    kmer.set(Integer.toString(Size_Kmer - 1) + ":" + Utilities.kmerToIndex(candidate.substring(0, Size_Kmer - 1), 1));
                    context.write(kmer, one);
                    kmerCounters[1]++;
                }
                if ( NPos<0 || NPos>Size_Kmer-3 ) {
                    kmer.set(Integer.toString(Size_Kmer - 2) + ":" + Utilities.kmerToIndex(candidate.substring(0, Size_Kmer - 2), 1));
                    context.write(kmer, one);
                    kmerCounters[2]++;
                }
            }

            context.write( new Text( Integer.toString( 0 ) + ":" + String.format("%20d", Size_Kmer   ) ), new IntWritable( kmerCounters[0] ) );
            context.write( new Text( Integer.toString( 0 ) + ":" + String.format("%20d", Size_Kmer-1 ) ), new IntWritable( kmerCounters[1] ) );
            context.write( new Text( Integer.toString( 0 ) + ":" + String.format("%20d", Size_Kmer-2 ) ), new IntWritable( kmerCounters[2] ) );
        }
        else if (segment.charAt(0)=='+') {
            String candidate = "";
            int NPos;
            String subKmer = "";

            for (int i=1; i<=segment.length()-Size_Kmer; i++) {
                if ( segment.charAt(i)=='N' ) continue;
                candidate = segment.substring(i, Size_Kmer + i);
                NPos = candidate.indexOf('N');
                if ( NPos<0 ) {
                    kmer.set(Integer.toString(Size_Kmer) + ":" + Utilities.kmerToIndex(candidate, 1));
                    context.write(kmer, one);
                    kmerCounters[0]++;
                }
                if ( NPos<0 || NPos>Size_Kmer-2 ) {
                    kmer.set(Integer.toString(Size_Kmer - 1) + ":" + Utilities.kmerToIndex(candidate.substring(0, Size_Kmer - 1), 1));
                    context.write(kmer, one);
                    kmerCounters[1]++;
                }
                if ( NPos<0 || NPos>Size_Kmer-3 ) {
                    kmer.set(Integer.toString(Size_Kmer - 2) + ":" + Utilities.kmerToIndex(candidate.substring(0, Size_Kmer - 2), 1));
                    context.write(kmer, one);
                    kmerCounters[2]++;
                }
            }

			if ( candidate.length() > 0 ) {
	            subKmer = candidate.substring( 1 );
    	        if ( subKmer.indexOf('N')<0 ) {
        	        kmer.set( Integer.toString( Size_Kmer-1 ) + ":" + Utilities.kmerToIndex(subKmer, 1) );
            	    context.write( kmer, one );
                	kmerCounters[1]++;
            	}

	            subKmer = candidate.substring( 2 );
    	        if ( subKmer.indexOf('N')<0 ) {
        	        kmer.set( Integer.toString( Size_Kmer-2 ) + ":" + Utilities.kmerToIndex(subKmer, 1) );
            	    context.write( kmer, one );
                	kmerCounters[2]++;
	            }

	            subKmer = candidate.substring( 1, Size_Kmer-1 );
    	        if ( subKmer.indexOf('N')<0 ) {
        	        kmer.set( Integer.toString( Size_Kmer-2 ) + ":" + Utilities.kmerToIndex(subKmer, 1) );
            	    context.write( kmer, one );
                	kmerCounters[2]++;
            	}
			}

            context.write( new Text( Integer.toString( 0 ) + ":" + String.format("%20d", Size_Kmer   ) ), new IntWritable( kmerCounters[0] ) );
            context.write( new Text( Integer.toString( 0 ) + ":" + String.format("%20d", Size_Kmer-1 ) ), new IntWritable( kmerCounters[1] ) );
            context.write( new Text( Integer.toString( 0 ) + ":" + String.format("%20d", Size_Kmer-2 ) ), new IntWritable( kmerCounters[2] ) );
        }
    }
}
