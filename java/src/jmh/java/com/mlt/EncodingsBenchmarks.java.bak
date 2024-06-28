package com.mlt;

import com.mlt.converter.encodings.EncodingUtils;
import com.mlt.converter.mvt.MvtUtils;
import com.mlt.decoder.vectorized.VectorizedDecodingUtils;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * RLE benchmarks
 * Delta benchmarks
 * FastPfor benchmarks
 * C -> P conversion benchmarks
 * */

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@Threads(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@Fork(value = 1)
public class EncodingsBenchmarks {
    private int numTotalValues = 0;
    private int numRuns = 5000;
    private int[] data = new int[numRuns * 2];


    /*@Setup
    public void setup() throws IOException {
        for (int i = 0; i < numRuns; i++) {
            data[i] = (int) (Math.random() * 10);
            data[i + numRuns] = (int) (Math.ceil(Math.random() * 15));
            numTotalValues += data[i];
        }
    }*/

    @Setup
    public void setup() throws IOException {
        //Path path = Paths.get("./src/jmh/java/com/mlt/data/rle_PartOffsets.csv");
        //Path path = Paths.get("./src/jmh/java/com/mlt/data/rle_class_ratio17.csv");
        Path path = Paths.get("./src/jmh/java/com/mlt/data/rle_id_ratio22_45k.csv");
        var lines = Files.lines(path);
        String data = lines.collect(Collectors.joining("\n"));
        lines.close();

        var parts = data.split(";");
        var v = Arrays.stream(parts).map(Integer::valueOf).mapToInt(i -> i).toArray();
        var encodedValues = EncodingUtils.encodeRle(v);
        var a = new ArrayList<>(encodedValues.getLeft());
        a.addAll(encodedValues.getRight());

        this.data = a.stream().mapToInt(i -> i).toArray();
        this.numTotalValues = v.length;
        this.numRuns = encodedValues.getLeft().size();
    }

    @Benchmark
    public int[] vectorizedRleDecoding(){
        return VectorizedDecodingUtils.decodeUnsignedRleVectorized(data, numRuns, numTotalValues);
    }

    @Benchmark
    public int[] scalarRleDecoding(){
        return VectorizedDecodingUtils.decodeUnsignedRLE2(data, numRuns, numTotalValues);
    }

    /*int[] decodeUnsignedRle(int[] values, int[] runs, int numTotalValues) {
        var decodedValues = new int[numTotalValues];
        var offset = 0;
        for (var i = 0; i < runs.length; i++) {
            var runLength = runs[i];
            var value = values[i];
            for (var j = offset; j < offset + runLength; j++) {
                decodedValues[j] = value;
            }
            offset += runLength;
        }

        return decodedValues;
    }

    //private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    int[] decodeUnsignedRleVectorized(int[] values, int[] runs, int numTotalValues) {
        var SPECIES = IntVector.SPECIES_PREFERRED;
        var overflow = runs[runs.length - 1] % SPECIES.length();
        var overflowDst = new int[numTotalValues + overflow];
        int pos = 0;
        var numRuns = runs.length;
        for (int run = 0; run < numRuns; run++) {
            int count = runs[run];
            IntVector runVector = IntVector.broadcast(SPECIES, values[run]);
            int i = 0;
            for (; i <= count; i += SPECIES.length()) {
                runVector.intoArray(overflowDst, pos + i);
            }
            pos += count;
        }

        var dst = new int[numTotalValues];
        System.arraycopy(overflowDst, 0, dst, 0, numTotalValues);
        return dst;
    }*/
}
