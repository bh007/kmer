package org.bh007.kmer;

public class Utilities {

    // Define the index corresponding to a specific kmer:
    // Let A = 0, C = 1, G = 2, T = 3;
    // A K-mer has K bits from right to left, like for example K = 5:
    // CTAGC = 1 * 4^0 + 3 * 4^1 + 0 * 4^2 + 2 * 4^3 + 1*4^4 = 397;
    public static String kmerToIndex( String kmer, int sw ) {
        int bit = 0, base4, value;
        int index = 0, begin = 0, end = 0;

        do {
            base4 = 1<<(2*bit);
            value = 0;
            if ( kmer.charAt(bit) == 'C' )
                value = base4;
            else if ( kmer.charAt(bit) == 'G' )
                value = base4 << 1;
            else if ( kmer.charAt(bit) == 'T' )
                value = 3 * base4;

            index += value;
            if (bit==0) begin = value;
            if (bit==kmer.length()-1) end = value;

        } while (++bit<kmer.length());

        String res = String.format( "%20d", index );
        if ( sw != 0)
            return res;
        else
            return res
                    + String.format("%20d",   index - end                )
                    + String.format("%20d", ( index - begin ) >> 2       )
                    + String.format("%20d", ( index - end - begin ) >> 2 );
    }

    public static void resolveIndex( int idx, int k, int[] subIdx ) {
        int residue, shift = 2 * ( k - 1 );

        subIdx[0] = idx % ( 1<<shift );
        residue = subIdx[0];
        do {
            shift -= 2;
            residue %= ( 1<<shift );
        } while ( shift > 2 );
        subIdx[1] = ( idx - residue ) >> 2;
        subIdx[2] = ( subIdx[0] - residue ) >> 2;
    }
}
