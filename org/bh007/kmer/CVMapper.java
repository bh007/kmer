package org.bh007.kmer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class CVMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    private IntWritable kmer = new IntWritable();
    private DoubleWritable component = new DoubleWritable();

    private int Size_Kmer;
    private int Size_Segment;

    private int[] numKmer = new int[3];
    private int[] cntKmer = new int[3];
    private int[] idxKmer1;
    private int[] frqKmer1;
    private int[] idxKmer2;
    private int[] frqKmer2;
    private double commFactor;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        Size_Kmer = Integer.parseInt( conf.get("Size.Kmer") );
        Size_Segment = Integer.parseInt( conf.get("Size.Segment") );

        BufferedReader br = null;
        String line;

        int cntLine = 0;
        br = new BufferedReader( new FileReader( "./kt" ) );
        while ( ( line = br.readLine() ) != null) {
            line = line.trim();
            if (line.length() == 0) continue;

            Scanner sc = new Scanner( line );
            sc.nextInt();
            cntKmer[2-cntLine] = sc.nextInt();
            numKmer[2-cntLine] = sc.nextInt();
            sc.close();
            cntLine++;
        }
        br.close();
        commFactor = cntKmer[1] * cntKmer[1] / (double) ( cntKmer[0] * cntKmer[2] );
        idxKmer1 = new int[numKmer[1]];
        frqKmer1 = new int[numKmer[1]];
        idxKmer2 = new int[numKmer[2]];
        frqKmer2 = new int[numKmer[2]];

        cntLine = 0;
        br = new BufferedReader( new FileReader( "./k1" ) );
        while ( ( line = br.readLine() ) != null) {
            line = line.trim();
            if (line.length() == 0) continue;

            Scanner sc = new Scanner( line );
            idxKmer1[cntLine] = sc.nextInt();
            frqKmer1[cntLine] = sc.nextInt();
            sc.close();
            cntLine++;
        }
        br.close();

        cntLine = 0;
        br = new BufferedReader( new FileReader( "./k2" ) );
        while ( ( line = br.readLine() ) != null) {
            line = line.trim();
            if (line.length() == 0) continue;

            Scanner sc = new Scanner( line );
            idxKmer2[cntLine] = sc.nextInt();
            frqKmer2[cntLine] = sc.nextInt();
            sc.close();
            cntLine++;
        }
        br.close();
    }

    @Override
    public void map( LongWritable key, Text value, Context context )
            throws IOException, InterruptedException {

        int index;
        int[] submerIndices = new int[3];
        double freq, freq1lf = 1, freq1rt = 1, freq2 = 1;
        Scanner sc = new Scanner( value.toString() );
        index         = sc.nextInt();
        freq          = (double) sc.nextInt();
        sc.close();
        Utilities.resolveIndex(index, Size_Kmer,submerIndices );

        int i;
        if ( ( i= Arrays.binarySearch(idxKmer1, submerIndices[0]) ) < 0 )
            System.out.println( "index_1_left: " + Integer.toString( submerIndices[0] ) ) ;
        else
            freq1lf = (double) frqKmer1[i];

        if ( ( i=Arrays.binarySearch( idxKmer1, submerIndices[1] ) ) < 0 )
            System.out.println( "index_1_right: " + Integer.toString( submerIndices[1] ) ) ;
        else
            freq1rt = (double) frqKmer1[i];

        if ( ( i=Arrays.binarySearch( idxKmer2, submerIndices[2] ) ) < 0 )
            System.out.println( "index_2: " + Integer.toString( submerIndices[2] ) ) ;
        else
            freq2   = (double) frqKmer2[i];

        //kmer.set( String.format( "%20d", index ) );
        kmer.set( index );
        component.set( commFactor * ( freq1lf * freq1rt ) / ( freq * freq2 ) - 1.);
        context.write( kmer, component );
    }
}