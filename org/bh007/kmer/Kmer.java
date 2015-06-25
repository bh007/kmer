package org.bh007.kmer;

import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Scanner;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.Counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;

public class Kmer extends Configured implements Tool {

    public static enum MyCounters {
        NKMER, NK_1MER, NK_2MER
    }

    public int run(String[] args) throws Exception {

        // JOB 1
        Configuration conf = getConf();

        Job job = Job.getInstance( conf, "KmerCounting" );

        FileSystem fs = FileSystem.get( conf );

        job.setJarByClass(Kmer.class);
        job.setMapperClass(KmerCountMapper.class);
        job.setPartitionerClass(KmerCountPartitioner.class);
        job.setCombinerClass(KmerCountCombiner.class);
        job.setReducerClass(KmerCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        int numReduceTasks = Integer.parseInt( conf.get("Number.Reducers") );
        job.setNumReduceTasks( numReduceTasks );

        Path inputPath  = new Path( args[0] );
        Path outputPath = new Path( args[1] );
		Path cachePath  = new Path( fs.getWorkingDirectory().toString() + "/" + args[2] );
        FileInputFormat.setInputPaths( job, inputPath );
        FileOutputFormat.setOutputPath( job, outputPath );

        LazyOutputFormat.setOutputFormatClass( job, TextOutputFormat.class );

        if( fs.exists( outputPath ) )
            fs.delete( outputPath, true );
		if( fs.exists( cachePath ) )
            fs.delete( cachePath, true );

        boolean success = job.waitForCompletion( true );

        long nkmer  = job.getCounters().findCounter( MyCounters.NKMER ).getValue();
        long nk1mer = job.getCounters().findCounter( MyCounters.NK_1MER ).getValue();
        long nk2mer = job.getCounters().findCounter( MyCounters.NK_2MER ).getValue();


        // JOB 2
        Configuration conf2 = getConf();

        FileSystem fs2 = FileSystem.get( conf2 );

        Job job2 = Job.getInstance( conf2, "CV" );

        job2.setJarByClass( Kmer.class );
        job2.setMapperClass( CVMapper.class );
        //job2.setPartitionerClass( CVPartitioner.class );
        //job2.setCombinerClass( CVCombiner.class );
        job2.setReducerClass(CVReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setNumReduceTasks(1);

        String ktPath = cachePath.toString() + "/KmerTotal";
        Path ktCache = new Path( ktPath + "/kt-r-00000" );
        BufferedReader bf = new BufferedReader( new InputStreamReader( fs2.open( ktCache ) ) );
        String str = "";
        str += bf.readLine().trim() + " " + Long.toString( nk2mer ) + "\n";
        str += bf.readLine().trim() + " " + Long.toString( nk1mer ) + "\n";
        str += bf.readLine().trim() + " " + Long.toString( nkmer ) + "\n";
        byte[] byt = str.getBytes();

        //if( fs2.exists( ktCache ) )
        //    fs2.delete( ktCache, true );
        FSDataOutputStream out = fs2.create( new Path( cachePath.toString() + "/kt" ) );
        out.write(byt);
        out.close();
        job2.addCacheFile( new URI( cachePath.toString() + "/kt#kt" ) );

        String k1Path = cachePath.toString() + "/K-1mer";
        Path k1 = new Path( cachePath.toString() + "/k1" );
        out = fs2.create( k1 );
        FileStatus[] status	= fs2.listStatus( new Path( k1Path ) );
        Path[] k1Cache = FileUtil.stat2Paths(status);
        FSDataInputStream in = null;
        for ( Path p : k1Cache ) {
            in = fs2.open( p );
            IOUtils.copyBytes(in, out, 4096, true);
            out = fs2.append( k1 );
        }
        out.close();
        job2.addCacheFile( new URI( cachePath.toString() + "/k1#k1" ) );

        String k2Path = cachePath.toString() + "/K-2mer";
        Path k2 = new Path( cachePath.toString() + "/k2" );
        out = fs2.create( k2 );
        status	= fs2.listStatus( new Path( k2Path ) );
        Path[] k2Cache = FileUtil.stat2Paths(status);
        for ( Path p : k2Cache ) {
            in = fs2.open( p );
            IOUtils.copyBytes(in, out, 4096, true);
            out = fs2.append( k2 );
        }
        out.close();
        job2.addCacheFile( new URI( cachePath.toString() + "/k2#k2" ) );

        String kmerPath = cachePath.toString() + "/K-mer";
        Path kmer = new Path( cachePath.toString() + "/kmer" );
        out = fs2.create( kmer );
        status	= fs2.listStatus( new Path( kmerPath ) );
        Path[] kmerCache = FileUtil.stat2Paths(status);
        for ( Path p : kmerCache ) {
            in = fs2.open( p );
            IOUtils.copyBytes(in, out, 4096, true);
            out = fs2.append( kmer );
        }
        out.close();

        inputPath = new Path( cachePath.toString() + "/K-mer" );
        FileInputFormat.setInputPaths( job2, inputPath );
        FileOutputFormat.setOutputPath( job2, outputPath );

        LazyOutputFormat.setOutputFormatClass( job2, TextOutputFormat.class );

        if( fs2.exists( outputPath ) )
            fs2.delete( outputPath, true );

        success = job2.waitForCompletion( true );
/*
        String cvPath = outputPath.toString();
        Path cv = new Path( cvPath + "/cv" );
        out = fs2.create( cv );
        status	= fs2.globStatus( new Path( cvPath + "/part-*" ) );
        Path[] cvCache = FileUtil.stat2Paths(status);
		//for ( Path p : cvCache ) System.out.println( p );
        for ( Path p : cvCache ) {
            in = fs2.open( p );
            IOUtils.copyBytes(in, out, 4096, true);
            out = fs2.append( cv );
        }
        out.close();
*/
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Kmer(), args);
        System.exit(res);
    }
}

