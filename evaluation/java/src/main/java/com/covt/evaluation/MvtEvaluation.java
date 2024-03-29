package com.covt.evaluation;

import com.covt.evaluation.compression.IntegerCompression;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import io.github.sebasbaumh.mapbox.vectortile.adapt.jts.MvtReader;
import io.github.sebasbaumh.mapbox.vectortile.adapt.jts.TagKeyValueMapConverter;
import org.apache.commons.lang3.ArrayUtils;
import org.davidmoten.hilbert.HilbertCurve;
import org.davidmoten.hilbert.SmallHilbertCurve;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.covt.evaluation.compression.IntegerCompression.varintEncode;

public class MvtEvaluation {
    private static final String ID_KEY = "id";

    private static final Map<String, Integer> GEOMETRY_TYPES = Map.of("Point", 0,
        "LineString", 1, "Polygon", 2, "MultiPoint", 3,
            "MultiLineString", 4, "MultiPolygon", 5
    );

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException {
        var layers = parseMvt();
        convertLayers(layers);
    }

    private static void convertLayers(List<Layer> layers) throws IOException {
        for(var layer : layers){
            System.out.println(layer.name() + " " + "----------------------------------------------------");
            //analyzeIds(layer);
            //analyzeGeometryTypes(layer);
            //analyzeTopology(layer);
            analyzeGeometry(layer);
            /*if(layer.name().equals("place")){
                //analyzePlaceProperties(layer);
                analyzePlacePropertiesWithDictionaryForEveryColumn(layer);
            }
            else{
                //analyzeProperties(layer);
            }*/
        }
    }

    //Test separate dictionary for every column
    private static void analyzePlacePropertiesWithDictionaryForEveryColumn(Layer layer) throws IOException {
        /*
         *
         * -> Present stream ->  RLE encoding and Bit-Vector
         * -> String
         *   -> Length stream
         *       -> Parquet
         *           -> Delta-length byte array -> we will take all the byte array lengths and encode them using delta encoding (DELTA_BINARY_PACKED)
         *   -> Data stream
         *       -> Parquet
         *           -> encoding -> RLE/Bit-Packing Hybrid -> The values are stored as integers using the RLE/Bit-Packing Hybrid encoding
         *       -> If the dictionary grows too big, whether in size or number of distinct values, the encoding will fall back to the plain encoding
         *       -> ORC
         *           -> String, char, and varchar columns may be encoded either using a dictionary encoding or a direct encoding
         *           -> A direct encoding should be preferred when there are many distinct values
         *           -> In all of the encodings, the PRESENT stream encodes whether the value is null
         *           -> The Java ORC writer automatically picks the encoding after the first row group (10,000 rows)
         *           -> For direct encoding the UTF-8 bytes are saved in the DATA stream and the length of each value is written into the LENGTH stream
         *           -> Direct encoding streams -> Present (Boolean RLE), Length (RLE V1), Data (UTF-8)
         *           -> Dictionary encoding streams -> Present (Boolean RLE), Length (RLE V2), Data (RLE V2), DictionaryData (UTF-8)
         *   -> Dictionary stream
         * -> Int
         *       -> ORC
         *           -> Direct encoding streams -> Present (Boolean RLE), Data (RLE V2)
         * -> Boolean
         *       -> ORC
         *           -> Present (Boolean RLE), Data (Boolean RLE)
         * -> Float and Double
         *       -> ORC
         *           -> Present (Boolean RLE), Data (IEEE 754 floating point representation)
         * */

        /*
         * What is the portion of the string properties in a Gzip buffer when advanced encoded and when plain encoded?
         * */
        var features = layer.features();
        var stringProperties = new HashMap<String, List<String>>();
        for(var feature : features){
            var properties = feature.properties();
            for(var property : properties.entrySet()){
                var propertyName = property.getKey();
                var propertyValue = property.getValue();
                //System.out.println(propertyName + ": " + propertyValue);
                if(propertyValue instanceof String){
                    /*
                     * -> class
                     *   -> low entropy -> rle encoding for data stream
                     * -> name
                     *   -> high entropy
                     *   -> some repetition
                     * -> brunnel
                     *   -> low entropy -> rle encoding for data stream
                     *   -> Common prefix -> Polen, Poland, Polonia, Pologne, Poljska, Polandia, Polska, Polonha, Polonya
                     *       -> Fast Static Symbol Table (FSST) endcoding?
                     *       -> Front compression?
                     *           -> is a type of delta encoding compression algorithm whereby common prefixes or suffixes and their lengths
                     *              are recorded so that they need not be duplicated
                     *           -> This algorithm is particularly well-suited for compressing sorted data, e.g., a list of words from a dictionary
                     * */

                    //start by writing to buffer plain for every column -> Gzip encode the plain encoding
                    //Use the ORC encoding for string -> Gzip encode and compare with plain encoding
                    if(!stringProperties.containsKey(propertyName)){
                        stringProperties.put(propertyName, Lists.newArrayList((String)propertyValue));
                    }
                    else{
                        stringProperties.get(propertyName).add((String)propertyValue);
                    }
                }
            }
        }

        var plainStreamBuffer = new ArrayList<Byte>();
        var dictionaryStreamBuffer = new ArrayList<Byte>();
        var varintDictionaryStreamBuffer = new ArrayList<Byte>();
        var parquetRleDictionaryStreamBuffer = new ArrayList<Byte>();
        var orcV1RleDictionaryStreamBuffer = new ArrayList<Byte>();
        var orcV2RleDictionaryStreamBuffer = new ArrayList<Byte>();
        for(var propertyColumn : stringProperties.entrySet()){
            var values = propertyColumn.getValue();

            var plainStream = plainEncode(values);
            var dictionarySteam = dictionaryEncode(values);
            var varintDictionaryStream = dictionaryVarintEncode(values);
            var rleDictionaryStreams = rleDictionaryEncode(values);
            plainStreamBuffer.addAll(plainStream);
            dictionaryStreamBuffer.addAll(dictionarySteam);
            varintDictionaryStreamBuffer.addAll(varintDictionaryStream);
            parquetRleDictionaryStreamBuffer.addAll(rleDictionaryStreams.get(0));
            orcV1RleDictionaryStreamBuffer.addAll(rleDictionaryStreams.get(1));
            orcV2RleDictionaryStreamBuffer.addAll(rleDictionaryStreams.get(2));
        }

        System.out.println("----------------------------------------------------------");
        System.out.println(String.format("plain: %s, dictionary: %s, varint dictionary: %s, parquet: %s, orc v1: %s, orc v2: %s",
                plainStreamBuffer.size(),  dictionaryStreamBuffer.size(), varintDictionaryStreamBuffer.size(), parquetRleDictionaryStreamBuffer.size(),
                orcV1RleDictionaryStreamBuffer.size(), orcV2RleDictionaryStreamBuffer.size()));
        System.out.println(String.format("plain Gzip: %s, dictionary Gzip: %s, varint dictionary Gzip: %s, " +
                        "parquet Gzip: %s, orc v1 Gzip: %s, orc v2 Gzip: %s",
                toGzip(plainStreamBuffer).length, toGzip(dictionaryStreamBuffer).length, toGzip(varintDictionaryStreamBuffer).length,
                toGzip(parquetRleDictionaryStreamBuffer).length, toGzip(orcV1RleDictionaryStreamBuffer).length, toGzip(orcV2RleDictionaryStreamBuffer).length));
        System.out.println("----------------------------------------------------------");
    }

    private static void analyzePlaceProperties(Layer layer) throws IOException {
        /*
         *
         * -> Present stream ->  RLE encoding and Bit-Vector
         * -> String
         *   -> Length stream
         *       -> Parquet
         *           -> Delta-length byte array -> we will take all the byte array lengths and encode them using delta encoding (DELTA_BINARY_PACKED)
         *   -> Data stream
         *       -> Parquet
         *           -> encoding -> RLE/Bit-Packing Hybrid -> The values are stored as integers using the RLE/Bit-Packing Hybrid encoding
         *          -> If the dictionary grows too big, whether in size or number of distinct values, the encoding will fall back to the plain encoding
         *       -> ORC
         *           -> String, char, and varchar columns may be encoded either using a dictionary encoding or a direct encoding
         *           -> A direct encoding should be preferred when there are many distinct values
         *           -> In all of the encodings, the PRESENT stream encodes whether the value is null
         *           -> The Java ORC writer automatically picks the encoding after the first row group (10,000 rows)
         *           -> For direct encoding the UTF-8 bytes are saved in the DATA stream and the length of each value is written into the LENGTH stream
         *           -> Direct encoding streams -> Present (Boolean RLE), Length (RLE V1), Data (UTF-8)
         *           -> Dictionary encoding streams -> Present (Boolean RLE), Length (RLE V2), Data (RLE V2), DictionaryData (UTF-8)
         *   -> Dictionary stream
         * -> Int
         *       -> ORC
         *           -> Direct encoding streams -> Present (Boolean RLE), Data (RLE V2)
         * -> Boolean
         *       -> ORC
         *           -> Present (Boolean RLE), Data (Boolean RLE)
         * -> Float and Double
         *       -> ORC
         *           -> Present (Boolean RLE), Data (IEEE 754 floating point representation)
         * */

        /*
         * What is the portion of the string properties in a Gzip buffer when advanced encoded and when plain encoded?
         * */
        //Column oriented
        var features = layer.features();
        var featureProperties = new TreeSet<String>();
        for(var feature : features){
            for(var property : feature.properties().keySet()){
                featureProperties.add(property);
            }
        }

        var values = new ArrayList<String>();
        var plainStream = new ArrayList<Byte>();
        for(var property : featureProperties){
            for(var feature : features){
                var properties = feature.properties();
                var propertyValue = properties.get(property);
                if(propertyValue instanceof String){
                    var value = (String)propertyValue;
                    var buffer = ArrayUtils.toObject(value.getBytes(StandardCharsets.UTF_8));
                    plainStream.addAll(Arrays.asList(buffer));
                    values.add(value);
                }
            }
        }

        //Row oriented
        /*var features = layer.features();
        var values = new ArrayList<String>();
        var plainStream = new ArrayList<Byte>();
        for(var feature : features){
            var properties = feature.properties();
            for(var property : properties.entrySet()){
                var propertyValue = property.getValue();
                if(propertyValue instanceof String){
                    var value = (String)propertyValue;
                    var buffer = ArrayUtils.toObject(value.getBytes(StandardCharsets.UTF_8));
                    plainStream.addAll(Arrays.asList(buffer));
                    values.add(value);
                }
            }
        }*/

        var dictionarySteam = dictionaryEncode(values);
        var varintDictionaryStream = dictionaryVarintEncode(values);
        var rleDictionaryStreams = rleDictionaryEncode(values);
        System.out.println("----------------------------------------------------------");
        System.out.println(String.format("plain: %s, dictionary: %s, varint dictionary: %s, parquet: %s, orc v1: %s, orc v2: %s",
                    plainStream.size(),  dictionarySteam.size(), varintDictionaryStream.size(), rleDictionaryStreams.get(0).size(),
                    rleDictionaryStreams.get(1).size(), rleDictionaryStreams.get(2).size()));
        System.out.println(String.format("plain Gzip: %s, dictionary Gzip: %s, varint dictionary Gzip: %s, " +
                            "parquet Gzip: %s, orc v1 Gzip: %s, orc v2 Gzip: %s",
                    toGzip(plainStream).length, toGzip(dictionarySteam).length, toGzip(varintDictionaryStream).length,
                    toGzip(rleDictionaryStreams.get(0)).length, toGzip(rleDictionaryStreams.get(1)).length, toGzip(rleDictionaryStreams.get(2)).length));
        System.out.println("----------------------------------------------------------");
    }

    private static void analyzeProperties(Layer layer) throws IOException {
        /*
        *
        * -> Present stream ->  RLE encoding and Bit-Vector
        * -> String
        *   -> Length stream
        *       -> Parquet
        *           -> Delta-length byte array -> we will take all the byte array lengths and encode them using delta encoding (DELTA_BINARY_PACKED)
        *   -> Data stream
        *       -> Parquet
        *           -> encoding -> RLE/Bit-Packing Hybrid -> The values are stored as integers using the RLE/Bit-Packing Hybrid encoding
            *       -> If the dictionary grows too big, whether in size or number of distinct values, the encoding will fall back to the plain encoding
        *       -> ORC
        *           -> String, char, and varchar columns may be encoded either using a dictionary encoding or a direct encoding
        *           -> A direct encoding should be preferred when there are many distinct values
        *           -> In all of the encodings, the PRESENT stream encodes whether the value is null
        *           -> The Java ORC writer automatically picks the encoding after the first row group (10,000 rows)
        *           -> For direct encoding the UTF-8 bytes are saved in the DATA stream and the length of each value is written into the LENGTH stream
        *           -> Direct encoding streams -> Present (Boolean RLE), Length (RLE V1), Data (UTF-8)
        *           -> Dictionary encoding streams -> Present (Boolean RLE), Length (RLE V2), Data (RLE V2), DictionaryData (UTF-8)
        *   -> Dictionary stream
        * -> Int
        *       -> ORC
        *           -> Direct encoding streams -> Present (Boolean RLE), Data (RLE V2)
        * -> Boolean
        *       -> ORC
        *           -> Present (Boolean RLE), Data (Boolean RLE)
        * -> Float and Double
        *       -> ORC
        *           -> Present (Boolean RLE), Data (IEEE 754 floating point representation)
        * */

        /*
        * What is the portion of the string properties in a Gzip buffer when advanced encoded and when plain encoded?
        * */
        var features = layer.features();
        var stringProperties = new HashMap<String, List<String>>();
        for(var feature : features){
            var properties = feature.properties();
            for(var property : properties.entrySet()){
                var propertyName = property.getKey();
                var propertyValue = property.getValue();
                //System.out.println(propertyName + ": " + propertyValue);
                if(propertyValue instanceof String){
                    /*
                    * -> class
                    *   -> low entropy -> rle encoding for data stream
                    * -> name
                    *   -> high entropy
                    *   -> some repetition
                    * -> brunnel
                    *   -> low entropy -> rle encoding for data stream
                    *   -> Common prefix -> Polen, Poland, Polonia, Pologne, Poljska, Polandia, Polska, Polonha, Polonya
                    *       -> Fast Static Symbol Table (FSST) endcoding?
                    *       -> Front compression?
                    *           -> is a type of delta encoding compression algorithm whereby common prefixes or suffixes and their lengths
                    *              are recorded so that they need not be duplicated
                    *           -> This algorithm is particularly well-suited for compressing sorted data, e.g., a list of words from a dictionary
                    * */

                    //start by writing to buffer plain for every column -> Gzip encode the plain encoding
                    //Use the ORC encoding for string -> Gzip encode and compare with plain encoding
                    if(!stringProperties.containsKey(propertyName)){
                        stringProperties.put(propertyName, Lists.newArrayList((String)propertyValue));
                    }
                    else{
                        stringProperties.get(propertyName).add((String)propertyValue);
                    }
                }
                else if(propertyValue instanceof Boolean){

                }
                else if(propertyValue instanceof Integer){

                }
                else if(propertyValue instanceof Long){

                }
                else if(propertyValue instanceof Float){

                }
                else if(propertyValue instanceof Double){

                }
                else{

                }
            }
        }

        for(var propertyColumn : stringProperties.entrySet()){
            var values = propertyColumn.getValue();

            var plainStream = plainEncode(values);
            if(plainStream.size() < 2000){
                continue;
            }

            var dictionarySteam = dictionaryEncode(values);
            var varintDictionaryStream = dictionaryVarintEncode(values);
            var rleDictionaryStreams = rleDictionaryEncode(values);
            System.out.println(propertyColumn.getKey() + " ----------------------------------------------------------");
            System.out.println(String.format("plain: %s, dictionary: %s, varint dictionary: %s, parquet: %s, orc v1: %s, orc v2: %s",
                    plainStream.size(),  dictionarySteam.size(), varintDictionaryStream.size(), rleDictionaryStreams.get(0).size(),
                    rleDictionaryStreams.get(1).size(), rleDictionaryStreams.get(2).size()));
            System.out.println(String.format("plain Gzip: %s, dictionary Gzip: %s, varint dictionary Gzip: %s, " +
                            "parquet Gzip: %s, orc v1 Gzip: %s, orc v2 Gzip: %s",
                    toGzip(plainStream).length, toGzip(dictionarySteam).length, toGzip(varintDictionaryStream).length,
                    toGzip(rleDictionaryStreams.get(0)).length, toGzip(rleDictionaryStreams.get(1)).length, toGzip(rleDictionaryStreams.get(2)).length));
            System.out.println("----------------------------------------------------------");
        }
    }

    private static ArrayList<Byte> plainEncode(List<String> values){
        var encodedColumnValues = new ArrayList<Byte>();
        for(var value : values){
            var buffer = ArrayUtils.toObject(value.getBytes(StandardCharsets.UTF_8));
            encodedColumnValues.addAll(Arrays.asList(buffer));
        }

        return encodedColumnValues;
    }

    private static List<ArrayList<Byte>> rleDictionaryEncode(List<String> values) throws IOException {
        var dictionarySet = new LinkedHashSet<String>();
        var dataIndex = new int[values.size()];
        var dictionaryStream = new ArrayList<Byte>();
        var i = 0;
        for(var value : values){
            if(!dictionarySet.contains(value)){
                var buffer = ArrayUtils.toObject(value.getBytes(StandardCharsets.UTF_8));
                var index = dictionarySet.size();
                dataIndex[i++] = index;
                dictionaryStream.addAll(Arrays.asList(buffer));
                dictionarySet.add(value);
            }
            else{
                var index = new ArrayList(dictionarySet).indexOf(value);
                dataIndex[i++] = index;
            }
        }

        var dataIndexLong = Arrays.stream(dataIndex).mapToLong(j -> j).toArray();
        // If all elements of the array are the same -> Parquet throw a exception
        if(Arrays.stream(dataIndexLong).allMatch(a -> a == dataIndexLong[0])){
            var emptyList = new ArrayList<Byte>();
            System.out.println("All elements are equal -> empty list returned");
            return List.of(emptyList, emptyList, emptyList);
        }

        var parquetRLEIndexStream = IntegerCompression.parquetRLEBitpackingHybridEncoding(dataIndex);
        var orcRLE1IndexStream = IntegerCompression.orcRleEncodingV1(dataIndexLong);
        var orcRLE2IndexStream = IntegerCompression.orcRleEncodingV2(dataIndexLong);

        var parquetStreams = new ArrayList(Bytes.asList(parquetRLEIndexStream));
        parquetStreams.addAll(dictionaryStream);
        var orcRLE1Stream = new ArrayList(Bytes.asList(orcRLE1IndexStream));
        orcRLE1Stream.addAll(dictionaryStream);
        var orcRLE2Stream = new ArrayList(Bytes.asList(orcRLE2IndexStream));
        orcRLE2Stream.addAll(dictionaryStream);

        return List.of(parquetStreams, orcRLE1Stream, orcRLE2Stream);
    }

    private static ArrayList<Byte> dictionaryEncode(List<String> values){
        var dictionarySet = new LinkedHashSet<String>();
        var dataStream = new ArrayList<Byte>();
        var dictionaryStream = new ArrayList<Byte>();
        for(var value : values){
            if(!dictionarySet.contains(value)){
                var buffer = ArrayUtils.toObject(value.getBytes(StandardCharsets.UTF_8));
                var index = dictionarySet.size();
                ByteBuffer b = ByteBuffer.allocate(4);
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putInt(index);
                dataStream.addAll(Arrays.asList(ArrayUtils.toObject(b.array())));
                dictionaryStream.addAll(Arrays.asList(buffer));
                dictionarySet.add(value);
            }
            else{
                var index = new ArrayList(dictionarySet).indexOf(value);
                ByteBuffer b = ByteBuffer.allocate(4);
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putInt(index);
                dataStream.addAll(Arrays.asList(ArrayUtils.toObject(b.array())));
            }
        }


        dataStream.addAll(dictionaryStream);
        return dataStream;
    }

    private static ArrayList<Byte> dictionaryVarintEncode(List<String> values) throws IOException {
        var dictionarySet = new LinkedHashSet<String>();
        var dataStream = new ArrayList<Byte>();
        var dictionaryStream = new ArrayList<Byte>();
        for(var value : values){
            if(!dictionarySet.contains(value)){
                var buffer = ArrayUtils.toObject(value.getBytes(StandardCharsets.UTF_8));
                var index = dictionarySet.size();
                //System.out.println(value + " " + index);
                var varintEncodedIndex = varintEncode(new int[]{index});
                dataStream.addAll(Arrays.asList(ArrayUtils.toObject(varintEncodedIndex)));
                dictionaryStream.addAll(Arrays.asList(buffer));
                dictionarySet.add(value);
            }
            else{
                var index = new ArrayList(dictionarySet).indexOf(value);
                //System.out.println(value + " " + index);
                ByteBuffer b = ByteBuffer.allocate(4);
                b.order(ByteOrder.LITTLE_ENDIAN);
                b.putInt(index);
                dataStream.addAll(Arrays.asList(ArrayUtils.toObject(b.array())));
            }
        }


        dataStream.addAll(dictionaryStream);
        return dataStream;
    }

    private static byte[] toGzip(ArrayList<Byte> encodedColumnValues) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzipOut = new GZIPOutputStream(baos);
        byte[] buffer = new byte[encodedColumnValues.size()];
        for(int i = 0; i < encodedColumnValues.size(); i++) {
            buffer[i] = encodedColumnValues.get(i).byteValue();
        }
        gzipOut.write(buffer);
        gzipOut.close();
        baos.close();

        return baos.toByteArray();
    }

    private static void analyzeVertexBuffer(Layer layer) throws IOException {
        var name = layer.name();
        if(!name.equals("transportation")){
            return;
        }

        /*
         * -> Sort Vertices on Hilbert Curve
         * -> Iterate of the coordinates of the geometries and compare with the geometries in the vertex buffer
         *    and save the index
         * -> Delta Encode Vertex Buffer
         * -> Delta Encode Index Buffer?
         * */

        //14 bits -> 8192 in two directions
        var numCoordinatesPerQuadrant = 8192;
        SmallHilbertCurve hilbertCurve =
                HilbertCurve.small().bits(14).dimensions(2);
        var vertexMap = new TreeMap<Integer, Vertex>();
        var features = layer.features();
        for(var feature : features){
            //var geometryType = GEOMETRY_TYPES.get(feature.geometry().getGeometryType());
            var geometryType = feature.geometry().getGeometryType();

            switch(geometryType){
                case "LineString": {
                    var lineString = (LineString) feature.geometry();
                    var vertices = lineString.getCoordinates();
                    for (var vertex : vertices) {
                        /* shift origin to have no negative coordinates */
                        var x = numCoordinatesPerQuadrant + (int) vertex.x;
                        var y = numCoordinatesPerQuadrant + (int) vertex.y;
                        var index = (int) hilbertCurve.index(x, y);
                        if (!vertexMap.containsKey(index)) {
                            vertexMap.put(index, new Vertex(x, y));
                        }
                    }

                    break;
                }
                case "MultiLineString":{
                    var multiLineString = ((MultiLineString)feature.geometry());
                    var numLineStrings = multiLineString.getNumGeometries();
                    for(var i = 0; i < numLineStrings; i++){
                        var lineString =  (LineString)multiLineString.getGeometryN(i);
                        var vertices = lineString.getCoordinates();
                        for (var vertex : vertices) {
                            /* shift origin to have no negative coordinates */
                            var x = numCoordinatesPerQuadrant + (int) vertex.x;
                            var y = numCoordinatesPerQuadrant + (int) vertex.y;
                            var index = (int) hilbertCurve.index(x, y);
                            if (!vertexMap.containsKey(index)) {
                                vertexMap.put(index, new Vertex(x, y));
                            }
                        }
                    }
                    break;
                }
            }
        }


        Set<Map.Entry<Integer, Vertex>> vertexSet = vertexMap.entrySet();
        var vertexOffsets = new ArrayList<Integer>();
        for(var feature : features){
            var geometryType = feature.geometry().getGeometryType();
            switch(geometryType){
                case "LineString": {
                    var lineString = (LineString) feature.geometry();
                    var vertices = lineString.getCoordinates();
                    for (var vertex : vertices) {
                        var x = numCoordinatesPerQuadrant + (int) vertex.x;
                        var y = numCoordinatesPerQuadrant + (int) vertex.y;
                        var hilbertIndex = (int) hilbertCurve.index(x, y);
                        var vertexOffset = Iterables.indexOf(vertexSet,v -> v.getKey().equals(hilbertIndex));
                        vertexOffsets.add(vertexOffset);
                    }

                    break;
                }
                case "MultiLineString":{
                    var multiLineString = ((MultiLineString)feature.geometry());
                    var numLineStrings = multiLineString.getNumGeometries();
                    for(var i = 0; i < numLineStrings; i++){
                        var lineString =  (LineString)multiLineString.getGeometryN(i);
                        var vertices = lineString.getCoordinates();
                        for (var vertex : vertices) {
                            /* shift origin to have no negative coordinates */
                            var x = numCoordinatesPerQuadrant + (int) vertex.x;
                            var y = numCoordinatesPerQuadrant + (int) vertex.y;
                            var hilbertIndex = (int) hilbertCurve.index(x, y);
                            var vertexOffset = Iterables.indexOf(vertexSet,v -> v.getKey().equals(hilbertIndex));
                            vertexOffsets.add(vertexOffset);
                        }
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Geometry type not supported.");
            }
        }

        var vertexBuffer = vertexSet.stream().flatMap(v -> {
            var coord = v.getValue();
            var x = coord.x();
            var y = coord.y();
            return Stream.of(x,y);
        }).mapToInt(i->i).toArray();
        var vertexOffsetsArr = vertexOffsets.stream().mapToInt(i->i).toArray();

        var vertexBufferParquetDelta = IntegerCompression.parquetDeltaEncoding(vertexBuffer);
        var vertexBufferORCRleV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(vertexBuffer).mapToLong(i -> i).toArray());
        var vertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(vertexOffsetsArr);
        var vertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(vertexOffsetsArr).mapToLong(i -> i).toArray());
        System.out.println("Ratio: " + ((double)vertexOffsetsArr.length / vertexBuffer.length));
        System.out.println("VertexBuffer Parquet Delta: " + vertexBufferParquetDelta.length / 1000);
        System.out.println("VertexBuffer ORC RLEV2: " + vertexBufferORCRleV2.length / 1000);
        System.out.println("VertexOffsets Parquet Delta: " + vertexOffsetsParquetDelta.length / 1000);
        System.out.println("VertexOffsets ORC RLEV2: " + vertexOffsetsORCRleV2.length / 1000);

        /**
         * Sorting the full vertexOffsets not working -> only the offsets of a LineString collection can be sorted
         * -> but in Transportation most of the time only 2 vertices
         * -> LineString -> PartOffsets (e.g. vertex 0 to 10) -> VertexOffsets (int to coordinates) -> VertexBuffer
         * -> LineString -> PartOffsets (e.g. vertex 0 to 10) -> HilbertIndices
         */
        Collections.sort(vertexOffsets);
        var sortedDeltaEncodedVertexOffsets = new long[vertexOffsetsArr.length];
        var previousValue = 0;
        for(var i = 0; i < vertexOffsets.size(); i++){
            var value = vertexOffsets.get(i);
            var delta = value - previousValue;
            sortedDeltaEncodedVertexOffsets[i] = delta;
            previousValue = value;
        }
        var sortedVertexOffsetsArr = vertexOffsets.stream().mapToInt(i->i).toArray();
        var sortedVertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(sortedVertexOffsetsArr);
        var sortedVertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(sortedVertexOffsetsArr).mapToLong(i -> i).toArray());
        var sortedDeltaVertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(Arrays.stream(sortedDeltaEncodedVertexOffsets).mapToInt(i -> (int)i).toArray());
        var sortedDeltaVertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(sortedDeltaEncodedVertexOffsets);
        System.out.println("Sorted VertexOffsets Parquet Delta: " + sortedVertexOffsetsParquetDelta.length / 1000);
        System.out.println("Sorted VertexOffsets ORC RLEV2: " + sortedVertexOffsetsORCRleV2.length / 1000);
        System.out.println("Delta Sorted VertexOffsets Parquet Delta: " + sortedDeltaVertexOffsetsParquetDelta.length / 1000);
        System.out.println("Delta Sorted VertexOffsets ORC RLEV2: " + sortedDeltaVertexOffsetsORCRleV2.length / 1000);

        /*var deltaEncodedVertexOffsets = new long[vertexOffsetsArr.length];
        var previousValue = 0;
        for(var i = 0; i < vertexOffsetsArr.length; i++){
            var value = vertexOffsetsArr[i];
            var delta = value - previousValue;
            deltaEncodedVertexOffsets[i] = delta;
            previousValue = value;
        }
        var deltaEncodedVertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(Arrays.stream(deltaEncodedVertexOffsets).mapToInt(i -> (int)i).toArray());
        var deltaEncodedVertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(deltaEncodedVertexOffsets);
        System.out.println("Delta VertexOffsets Parquet Delta: " + deltaEncodedVertexOffsetsParquetDelta.length / 1000);
        System.out.println("Delta Vertex Offsets ORC RLEV2: " + deltaEncodedVertexOffsetsORCRleV2.length / 1000);*/
    }

    private static void analyzeGeometry(Layer layer) throws IOException {
        var name = layer.name();
        if(!name.equals("transportation")){
            return;
        }

        /*
         * -> Sort Vertices on Hilbert Curve
         * -> Iterate of the coordinates of the geometries and compare with the geometries in the vertex buffer
         *    and save the index
         * -> Delta Encode Vertex Buffer
         * -> Delta Encode Index Buffer?
         * */

        //14 bits -> 8192 in two directions
        var numCoordinatesPerQuadrant = 8192;
        SmallHilbertCurve hilbertCurve =
                HilbertCurve.small().bits(14).dimensions(2);
        var vertexMap = new TreeMap<Integer, Vertex>();
        var features = layer.features();
        for(var feature : features){
            //var geometryType = GEOMETRY_TYPES.get(feature.geometry().getGeometryType());
            var geometryType = feature.geometry().getGeometryType();

            switch(geometryType){
                case "Point":
                    break;
                case "LineString": {
                    var lineString = (LineString) feature.geometry();
                    var vertices = lineString.getCoordinates();
                    for (var vertex : vertices) {
                        /* shift origin to have no negative coordinates */
                        var x = numCoordinatesPerQuadrant + (int) vertex.x;
                        var y = numCoordinatesPerQuadrant + (int) vertex.y;
                        var index = (int) hilbertCurve.index(x, y);
                        if (!vertexMap.containsKey(index)) {
                            vertexMap.put(index, new Vertex(x, y));
                        }
                    }

                    break;
                }
                case "Polygon":
                    break;
                case "MultiPoint":
                    throw new IllegalArgumentException("Geometry type MultiPoint is not supported yet.");
                case "MultiLineString":{
                    var multiLineString = ((MultiLineString)feature.geometry());
                    var numLineStrings = multiLineString.getNumGeometries();
                    for(var i = 0; i < numLineStrings; i++){
                        var lineString =  (LineString)multiLineString.getGeometryN(i);
                        var vertices = lineString.getCoordinates();
                        for (var vertex : vertices) {
                            /* shift origin to have no negative coordinates */
                            var x = numCoordinatesPerQuadrant + (int) vertex.x;
                            var y = numCoordinatesPerQuadrant + (int) vertex.y;
                            var index = (int) hilbertCurve.index(x, y);
                            if (!vertexMap.containsKey(index)) {
                                vertexMap.put(index, new Vertex(x, y));
                            }
                        }
                    }
                    break;
                }
                case "MultiPolygon":
                    break;
                default:
                    throw new IllegalArgumentException("GeometryCollection not supported.");
            }
        }


        Set<Map.Entry<Integer, Vertex>> vertexSet = vertexMap.entrySet();
        var vertexOffsets = new ArrayList<Integer>();
        for(var feature : features){
            var geometryType = feature.geometry().getGeometryType();
            switch(geometryType){
                case "LineString": {
                    var lineString = (LineString) feature.geometry();
                    var vertices = lineString.getCoordinates();
                    for (var vertex : vertices) {
                        var x = numCoordinatesPerQuadrant + (int) vertex.x;
                        var y = numCoordinatesPerQuadrant + (int) vertex.y;
                        var hilbertIndex = (int) hilbertCurve.index(x, y);
                        var vertexOffset = Iterables.indexOf(vertexSet,v -> v.getKey().equals(hilbertIndex));
                        vertexOffsets.add(vertexOffset);
                    }

                    break;
                }
                case "MultiLineString":{
                    var multiLineString = ((MultiLineString)feature.geometry());
                    var numLineStrings = multiLineString.getNumGeometries();
                    for(var i = 0; i < numLineStrings; i++){
                        var lineString =  (LineString)multiLineString.getGeometryN(i);
                        var vertices = lineString.getCoordinates();
                        for (var vertex : vertices) {
                            /* shift origin to have no negative coordinates */
                            var x = numCoordinatesPerQuadrant + (int) vertex.x;
                            var y = numCoordinatesPerQuadrant + (int) vertex.y;
                            var hilbertIndex = (int) hilbertCurve.index(x, y);
                            var vertexOffset = Iterables.indexOf(vertexSet,v -> v.getKey().equals(hilbertIndex));
                            vertexOffsets.add(vertexOffset);
                        }
                    }
                    break;
                }
                default:
                    throw new IllegalArgumentException("Geometry type not supported.");
            }
        }

        var vertexBuffer = vertexSet.stream().flatMap(v -> {
            var coord = v.getValue();
            var x = coord.x();
            var y = coord.y();
            return Stream.of(x,y);
        }).mapToInt(i->i).toArray();
        var vertexOffsetsArr = vertexOffsets.stream().mapToInt(i->i).toArray();

        var vertexBufferParquetDelta = IntegerCompression.parquetDeltaEncoding(vertexBuffer);
        var vertexBufferORCRleV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(vertexBuffer).mapToLong(i -> i).toArray());
        var vertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(vertexOffsetsArr);
        var vertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(vertexOffsetsArr).mapToLong(i -> i).toArray());
        System.out.println("Ratio: " + ((double)vertexOffsetsArr.length / vertexBuffer.length));
        System.out.println("VertexBuffer Parquet Delta: " + vertexBufferParquetDelta.length / 1000 + " kb");
        System.out.println("VertexBuffer ORC RLEV2: " + vertexBufferORCRleV2.length / 1000 + " kb");
        System.out.println("VertexOffsets Parquet Delta: " + vertexOffsetsParquetDelta.length / 1000 + " kb");
        System.out.println("VertexOffsets ORC RLEV2: " + vertexOffsetsORCRleV2.length / 1000 + " kb");

        /**
         * Sorting the full vertexOffsets not working -> only the offsets of a LineString collection can be sorted
         * -> but in Transportation most of the time only 2 vertices
         * -> LineString -> PartOffsets (e.g. vertex 0 to 10) -> VertexOffsets (int to coordinates) -> VertexBuffer
         * -> LineString -> PartOffsets (e.g. vertex 0 to 10) -> HilbertIndices
         */
        Collections.sort(vertexOffsets);
        var sortedDeltaEncodedVertexOffsets = new long[vertexOffsetsArr.length];
        var previousValue = 0;
        for(var i = 0; i < vertexOffsets.size(); i++){
            var value = vertexOffsets.get(i);
            var delta = value - previousValue;
            sortedDeltaEncodedVertexOffsets[i] = delta;
            previousValue = value;
        }
        var sortedVertexOffsetsArr = vertexOffsets.stream().mapToInt(i->i).toArray();
        var sortedVertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(sortedVertexOffsetsArr);
        var sortedVertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(sortedVertexOffsetsArr).mapToLong(i -> i).toArray());
        var sortedDeltaVertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(Arrays.stream(sortedDeltaEncodedVertexOffsets).mapToInt(i -> (int)i).toArray());
        var sortedDeltaVertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(sortedDeltaEncodedVertexOffsets);
        System.out.println("Sorted VertexOffsets Parquet Delta: " + sortedVertexOffsetsParquetDelta.length / 1000 + " kb");
        System.out.println("Sorted VertexOffsets ORC RLEV2: " + sortedVertexOffsetsORCRleV2.length / 1000 + " kb");
        System.out.println("Delta Sorted VertexOffsets Parquet Delta: " + sortedDeltaVertexOffsetsParquetDelta.length / 1000 + " kb");
        System.out.println("Delta Sorted VertexOffsets ORC RLEV2: " + sortedDeltaVertexOffsetsORCRleV2.length / 1000 + " kb");

        /*var deltaEncodedVertexOffsets = new long[vertexOffsetsArr.length];
        var previousValue = 0;
        for(var i = 0; i < vertexOffsetsArr.length; i++){
            var value = vertexOffsetsArr[i];
            var delta = value - previousValue;
            deltaEncodedVertexOffsets[i] = delta;
            previousValue = value;
        }
        var deltaEncodedVertexOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(Arrays.stream(deltaEncodedVertexOffsets).mapToInt(i -> (int)i).toArray());
        var deltaEncodedVertexOffsetsORCRleV2 = IntegerCompression.orcRleEncodingV2(deltaEncodedVertexOffsets);
        System.out.println("Delta VertexOffsets Parquet Delta: " + deltaEncodedVertexOffsetsParquetDelta.length / 1000);
        System.out.println("Delta Vertex Offsets ORC RLEV2: " + deltaEncodedVertexOffsetsORCRleV2.length / 1000);*/
    }

    private static void analyzeTopology(Layer layer) throws IOException {
        var name = layer.name();
        /*if(!name.equals("transportation")){
            return;
        }*/

        /*
        * Depending on the geometry type the topology column has the following streams:
        * - Point: no stream
        * - LineString: Part offsets
        * - Polygon: Part offsets (Polygon), Ring offsets (LinearRing)
        * - MultiPoint: Geometry offsets -> array of offsets indicate where the vertices of each MultiPoint start
        * - MultiLineString: Geometry offsets, Part offsets (LineString)
        * - MultiPolygon -> Geometry offsets, Part offsets (Polygon), Ring offsets (LinearRing)
        * Currently for all geometry types all streams hava an entry -> later flags should be used to
        * reduce memory footprint in the client after RLE decoding
        * -> If GeometrieOffsets >= 1 -> MulitPartGeometry
        * -> If PartOffsets and RingOffsets = 0 -> Point
        * -> If PartOffsets >= 2 and RingOffsets and GeometrieOffsets = 0 -> LinearRing
        * -> If GeometrieOffsets = 0 and RingOffsets >= 1 -> Polygon -> 0,1,10
        * -> Zero indicates no offsets (PartOffsets and RingOffsets) present
        *   -> A mulit-part geometry therefore needs at least one element
        *   -> against the SFA spec where zero and empty geometries are allowed
        * How to handle if there are different geometry types per layer like Point and LineString
        * not only the geometry and multi-part version of the geometry?
        * - Point and MultiPoint -> numPoints
        * - LineString and MultiLineString -> numLineStrings, numVertices
        * - Polygon and MultiPolygon -> numPolygons, numRings, numVertices
        * - Point and LineString -> numVertices -> 1 indicates Point geometry
        * - Point and Polygon -> numRings, numVertices -> 1 for numRings and numVertices indicates Point
        * - LineString and Polygon
        * - Point and MultiLineString
        * - Point and MultiPolygon
        * - LineString and MultiPolygon
        * - LineString and MultiPoint
        * - Polygon and MultiPoint
        * - Polygon and MultiLineString
        * -> if a fixed structure for every geometry with Geometry offsets, Part offsets (Polygon) and Ring offsets
        *    is used no geometry type column is needed
        * -> via RLE encoding the sparse columns like Geometry offsets and Part offsets can be effectively compressed
        * -> or use separate flag for layer which all have the same geometry type to prune columns -> less memory
        *    needed on the client -> find the typ with the highest dimension -> dimension flag for each layer
        * -> if Indexed Coordinate Encoding (ICE) is used -> additional vertex offset stream
        *   -> GeometryOffsets, PartOffsets, RingOffsets, VertexOffsets
        *  */

        var features = layer.features();
        var geometryOffsets = new ArrayList<Integer>();
        var partOffsets = new ArrayList<Integer>();
        var ringOffsets = new ArrayList<Integer>();
        for(var feature : features){
            //var geometryType = GEOMETRY_TYPES.get(feature.geometry().getGeometryType());
            var geometryType = feature.geometry().getGeometryType();

            switch(geometryType){
                case "Point":
                    break;
                case "LineString":
                    /*
                    *  Vertex offsets -> if Indexed Coordinate Encoding (ICE) is used
                    * */
                    partOffsets.add(feature.geometry().getNumPoints());
                    break;
                case "Polygon":

                    var polygon = (Polygon)feature.geometry();
                    var numRings = polygon.getNumInteriorRing() + 1;
                    partOffsets.add(numRings);
                    ringOffsets.add(polygon.getExteriorRing().getNumPoints());
                    for(var j = 0; j < polygon.getNumInteriorRing(); j++){
                        ringOffsets.add(polygon.getInteriorRingN(j).getNumPoints());
                    }
                    break;
                case "MultiPoint":
                    throw new IllegalArgumentException("Geometry type MultiPoint is not supported yet.");
                case "MultiLineString":
                    var multiLineString = ((MultiLineString)feature.geometry());
                    var numLineStrings = multiLineString.getNumGeometries();
                    geometryOffsets.add(numLineStrings);
                    for(var i = 0; i < numLineStrings; i++){
                        var numVertices = multiLineString.getGeometryN(i).getNumGeometries();
                        partOffsets.add(numVertices);
                    }
                    break;
                case "MultiPolygon":
                    var multiPolygon = ((MultiPolygon) feature.geometry());
                    var numPolygons = multiPolygon.getNumGeometries();
                    geometryOffsets.add(numPolygons);

                    for (var i = 0; i < numPolygons; i++) {
                        polygon = (Polygon) multiPolygon.getGeometryN(i);
                        numRings = polygon.getNumInteriorRing() + 1;
                        partOffsets.add(numRings);
                        ringOffsets.add(polygon.getExteriorRing().getNumPoints());
                        for(var j = 0; j < polygon.getNumInteriorRing(); j++){
                            ringOffsets.add(polygon.getInteriorRingN(j).getNumPoints());
                        }
                    }
                    break;
                default:
                    throw new IllegalArgumentException("GeometryCollection not supported.");
            }
        }

        var geometryOffsetsArr =  geometryOffsets.stream().mapToInt(i -> i).toArray();
        var partOffsetsArr =  partOffsets.stream().mapToInt(i -> i).toArray();
        var ringOffsetsArr =  ringOffsets.stream().mapToInt(i -> i).toArray();
        var geometryOffsetsRLEV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(geometryOffsetsArr).mapToLong(i -> i).toArray());
        var partOffsetsRLEV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(partOffsetsArr).mapToLong(i -> i).toArray());
        var ringOffsetsRLEV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(ringOffsetsArr).mapToLong(i -> i).toArray());
        var geometryOffsetsRLEV1 = IntegerCompression.orcRleEncodingV1(Arrays.stream(geometryOffsetsArr).mapToLong(i -> i).toArray());
        var partOffsetsRLEV1 = IntegerCompression.orcRleEncodingV1(Arrays.stream(partOffsetsArr).mapToLong(i -> i).toArray());
        var ringOffsetsRLEV1 = IntegerCompression.orcRleEncodingV1(Arrays.stream(ringOffsetsArr).mapToLong(i -> i).toArray());
        /*var geometryOffsetsParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(geometryOffsetsArr);*/
        var partOffsetsParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(partOffsetsArr);
        /*var ringOffsetsParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(ringOffsetsArr);*/
        var geometryOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(geometryOffsetsArr);
        var partOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(partOffsetsArr);
        var ringOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(ringOffsetsArr);
        System.out.println(name + " -----------------------------------------");
        System.out.println("GeometryOffsets RLE V2: " + geometryOffsetsRLEV2.length);
        System.out.println("GeometryOffsets RLE V1: " + geometryOffsetsRLEV1.length);
        System.out.println("GeometryOffsets Parquet Delta: " + geometryOffsetsParquetDelta.length);
        System.out.println("PartOffsets RLE V1: " + partOffsetsRLEV1.length);
        System.out.println("PartOffsets RLE V2: " + partOffsetsRLEV2.length);
        System.out.println("PartOffsets Parquet RLE: " + partOffsetsParquetRLE.length);
        /*System.out.println("RingOffsets Parquet RLE: " + ringOffsetsParquetRLE.length);*/
        System.out.println("PartOffsets Parquet Delta: " + partOffsetsParquetDelta.length);
        System.out.println("RingOffsets RLE V1: " + ringOffsetsRLEV1.length);
        System.out.println("RingOffsets RLE V2: " + ringOffsetsRLEV2.length);
        /*System.out.println("GeometryOffsets Parquet RLE: " + geometryOffsetsParquetRLE.length);*/
        System.out.println("RingOffsets Parquet Delta: " + ringOffsetsParquetDelta.length);

        var deltaEncodedPartOffsetsArr = new long[partOffsets.size()];
        var previousValue = 0;
        for(var i = 0; i < partOffsets.size(); i++){
            var value = partOffsets.get(i);
            var delta = value - previousValue;
            deltaEncodedPartOffsetsArr[i] = delta;
            previousValue = value;
        }
        var partOffsetsDeltaRLEV1 = IntegerCompression.orcRleEncodingV1(deltaEncodedPartOffsetsArr);
        var partOffsetsDeltaRLEV2 = IntegerCompression.orcRleEncodingV2(deltaEncodedPartOffsetsArr);
        var partOffsetsDeltaParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(Arrays.stream(deltaEncodedPartOffsetsArr).mapToInt(i -> (int)i).toArray());
        var partOffsetsDeltaParquetDelta = IntegerCompression.parquetDeltaEncoding(Arrays.stream(deltaEncodedPartOffsetsArr).mapToInt(i -> (int)i).toArray());
        System.out.println("PartOffsets Delta ORC RLE V1: " + partOffsetsDeltaRLEV1.length);
        System.out.println("PartOffsets Delta ORC RLE V2: " + partOffsetsDeltaRLEV2.length);
        System.out.println("PartOffsets Delta Parquet RLE: " + partOffsetsDeltaParquetRLE.length);
        System.out.println("PartOffsets Delta Parquet Delta: " + partOffsetsDeltaParquetDelta.length);
    }

    private static void analyzeTopologyOffsetBased(Layer layer) throws IOException {
        var name = layer.name();
        if(!name.equals("transportation")){
            return;
        }

        /*
         * Depending on the geometry type the topology column has the following streams:
         * - Point: no stream
         * - LineString: Part offsets
         * - Polygon: Part offsets (Polygon), Ring offsets (LinearRing)
         * - MultiPoint: Geometry offsets -> array of offsets indicate where the vertices of each MultiPoint start
         * - MultiLineString: Geometry offsets, Part offsets (LineString)
         * - MultiPolygon -> Geometry offsets, Part offsets (Polygon), Ring offsets (LinearRing)
         * Currently for all geometry types all streams hava an entry -> later flags should be used to
         * reduce memory footprint in the client after RLE decoding
         * -> If GeometrieOffsets >= 1 -> MulitPartGeometry
         * -> If PartOffsets and RingOffsets = 0 -> Point
         * -> If PartOffsets >= 2 and RingOffsets and GeometrieOffsets = 0 -> LinearRing
         * -> If GeometrieOffsets = 0 and RingOffsets >= 1 -> Polygon -> 0,1,10
         * -> Zero indicates no offsets (PartOffsets and RingOffsets) present
         *   -> A mulit-part geometry therefore needs at least one element
         *   -> against the SFA spec where zero and empty geometries are allowed
         * How to handle if there are different geometry types per layer like Point and LineString
         * not only the geometry and multi-part version of the geometry?
         * - Point and MultiPoint -> numPoints
         * - LineString and MultiLineString -> numLineStrings, numVertices
         * - Polygon and MultiPolygon -> numPolygons, numRings, numVertices
         * - Point and LineString -> numVertices -> 1 indicates Point geometry
         * - Point and Polygon -> numRings, numVertices -> 1 for numRings and numVertices indicates Point
         * - LineString and Polygon
         * - Point and MultiLineString
         * - Point and MultiPolygon
         * - LineString and MultiPolygon
         * - LineString and MultiPoint
         * - Polygon and MultiPoint
         * - Polygon and MultiLineString
         * -> if a fixed structure for every geometry with Geometry offsets, Part offsets (Polygon) and Ring offsets
         *    is used no geometry type column is needed
         * -> via RLE encoding the sparse columns like Geometry offsets and Part offsets can be effectively compressed
         * -> or use separate flag for layer which all have the same geometry type to prune columns -> less memory
         *    needed on the client -> find the typ with the highest dimension -> dimension flag for each layer
         * -> if Indexed Coordinate Encoding (ICE) is used -> additional vertex offset stream
         *   -> GeometryOffsets, PartOffsets, RingOffsets, VertexOffsets
         *  */

        var features = layer.features();
        var geometryOffsets = new ArrayList<Integer>();
        var partOffsets = new ArrayList<Integer>();
        var ringOffsets = new ArrayList<Integer>();
        geometryOffsets.add(0);
        partOffsets.add(0);
        ringOffsets.add(0);
        for(var feature : features){
            //var geometryType = GEOMETRY_TYPES.get(feature.geometry().getGeometryType());
            var geometryType = feature.geometry().getGeometryType();

            switch(geometryType){
                case "Point":
                    partOffsets.add(0);
                    ringOffsets.add(0);
                    geometryOffsets.add(0);
                    break;
                case "LineString":
                    /*
                     *  Vertex offsets -> if Indexed Coordinate Encoding (ICE) is used
                     * */
                    ringOffsets.add(0);
                    geometryOffsets.add(0);
                    var numVertices = feature.geometry().getNumPoints();
                    var partOffset = partOffsets.get(partOffsets.size() - 1) + numVertices;
                    partOffsets.add(partOffset);
                    break;
                case "Polygon":
                    geometryOffsets.add(0);

                    var polygon = (Polygon)feature.geometry();
                    var numRings = polygon.getNumInteriorRing() + 1;
                    partOffset = partOffsets.get(partOffsets.size() - 1) + numRings;
                    partOffsets.add(partOffset);
                    numVertices = polygon.getExteriorRing().getNumPoints();
                    for(var j = 0; j < polygon.getNumInteriorRing(); j++){
                        numVertices += polygon.getInteriorRingN(j).getNumPoints();
                    }
                    var ringOffset = ringOffsets.get(ringOffsets.size()-1) + numVertices;
                    ringOffsets.add(ringOffset);
                    break;
                case "MultiPoint":
                    throw new IllegalArgumentException("Geometry type MultiPoint is not supported yet.");
                case "MultiLineString":
                    ringOffsets.add(0);

                    var multiLineString = ((MultiLineString)feature.geometry());
                    var numLineStrings = multiLineString.getNumGeometries();
                    var geometryOffset = geometryOffsets.get(geometryOffsets.size() -1) + numLineStrings;
                    geometryOffsets.add(geometryOffset);
                    for(var i = 0; i < numLineStrings; i++){
                        numVertices = multiLineString.getGeometryN(i).getNumGeometries();
                        partOffset = partOffsets.get(partOffsets.size() - 1) + numVertices;
                        partOffsets.add(partOffset);
                    }
                    //TODO: how to handle the ring offsets stream?
                    break;
                case "MultiPolygon":
                    var multiPolygon = ((MultiPolygon) feature.geometry());
                    var numPolygons = multiPolygon.getNumGeometries();
                    geometryOffset = geometryOffsets.get(geometryOffsets.size() -1) + numPolygons;
                    geometryOffsets.add(geometryOffset);

                    for (var i = 0; i < numPolygons; i++) {
                        polygon = (Polygon) multiPolygon.getGeometryN(i);
                        numRings = polygon.getNumInteriorRing() + 1;
                        partOffset = partOffsets.get(partOffsets.size() - 1) + numRings;
                        partOffsets.add(partOffset);
                        numVertices = polygon.getExteriorRing().getNumPoints();
                        for (var j = 0; j < polygon.getNumInteriorRing(); j++) {
                            numVertices += polygon.getInteriorRingN(j).getNumPoints();
                        }
                        ringOffset = ringOffsets.get(ringOffsets.size()-1) + numVertices;
                        ringOffsets.add(ringOffset);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("GeometryCollection not supported.");
            }
        }

        var geometryOffsetsArr =  geometryOffsets.stream().mapToInt(i -> i).toArray();
        var partOffsetsArr =  partOffsets.stream().mapToInt(i -> i).toArray();
        var ringOffsetsArr =  ringOffsets.stream().mapToInt(i -> i).toArray();
        var geometryOffsetsRLEV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(geometryOffsetsArr).mapToLong(i -> i).toArray());
        var partOffsetsRLEV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(partOffsetsArr).mapToLong(i -> i).toArray());
        var ringOffsetsRLEV2 = IntegerCompression.orcRleEncodingV2(Arrays.stream(ringOffsetsArr).mapToLong(i -> i).toArray());
        var geometryOffsetsRLEV1 = IntegerCompression.orcRleEncodingV1(Arrays.stream(geometryOffsetsArr).mapToLong(i -> i).toArray());
        var partOffsetsRLEV1 = IntegerCompression.orcRleEncodingV1(Arrays.stream(partOffsetsArr).mapToLong(i -> i).toArray());
        var ringOffsetsRLEV1 = IntegerCompression.orcRleEncodingV1(Arrays.stream(ringOffsetsArr).mapToLong(i -> i).toArray());
        /*var geometryOffsetsParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(geometryOffsetsArr);*/
        var partOffsetsParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(partOffsetsArr);
        /*var ringOffsetsParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(ringOffsetsArr);*/
        var geometryOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(geometryOffsetsArr);
        var partOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(partOffsetsArr);
        var ringOffsetsParquetDelta = IntegerCompression.parquetDeltaEncoding(ringOffsetsArr);
        System.out.println(name + " -----------------------------------------");
        System.out.println("GeometryOffsets RLE V1: " + geometryOffsetsRLEV1.length);
        System.out.println("PartOffsets RLE V1: " + partOffsetsRLEV1.length);
        System.out.println("RingOffsets RLE V1: " + ringOffsetsRLEV1.length);
        System.out.println("GeometryOffsets RLE V2: " + geometryOffsetsRLEV2.length);
        System.out.println("PartOffsets RLE V2: " + partOffsetsRLEV2.length);
        System.out.println("RingOffsets RLE V2: " + ringOffsetsRLEV2.length);
        /*System.out.println("GeometryOffsets Parquet RLE: " + geometryOffsetsParquetRLE.length);*/
        System.out.println("PartOffsets Parquet RLE: " + partOffsetsParquetRLE.length);
        /*System.out.println("RingOffsets Parquet RLE: " + ringOffsetsParquetRLE.length);*/
        System.out.println("GeometryOffsets Parquet Delta: " + geometryOffsetsParquetDelta.length);
        System.out.println("PartOffsets Parquet Delta: " + partOffsetsParquetDelta.length);
        System.out.println("RingOffsets Parquet Delta: " + ringOffsetsParquetDelta.length);

        var deltaEncodedPartOffsetsArr = new long[partOffsets.size()];
        var previousValue = 0;
        for(var i = 0; i < partOffsets.size(); i++){
            var value = partOffsets.get(i);
            var delta = value - previousValue;
            deltaEncodedPartOffsetsArr[i] = delta;
            previousValue = value;
        }
        var partOffsetsDeltaRLEV1 = IntegerCompression.orcRleEncodingV1(deltaEncodedPartOffsetsArr);
        var partOffsetsDeltaRLEV2 = IntegerCompression.orcRleEncodingV2(deltaEncodedPartOffsetsArr);
        var partOffsetsDeltaParquetRLE = IntegerCompression.parquetRLEBitpackingHybridEncoding(Arrays.stream(deltaEncodedPartOffsetsArr).mapToInt(i -> (int)i).toArray());
        var partOffsetsDeltaParquetDelta = IntegerCompression.parquetDeltaEncoding(Arrays.stream(deltaEncodedPartOffsetsArr).mapToInt(i -> (int)i).toArray());
        System.out.println("PartOffsets Delta ORC RLE V1: " + partOffsetsDeltaRLEV1.length);
        System.out.println("PartOffsets Delta ORC RLE V2: " + partOffsetsDeltaRLEV2.length);
        System.out.println("PartOffsets Delta Parquet RLE: " + partOffsetsDeltaParquetRLE.length);
        System.out.println("PartOffsets Delta Parquet Delta: " + partOffsetsDeltaParquetDelta.length);
    }

    private static void analyzeGeometryTypes(Layer layer) throws IOException {
        var name = layer.name();
        long previousGeometryType = 0;
        var features = layer.features();
        var geometryTypes = new long[features.size()];
        var deltaGeometryTypes = new long[features.size()];
        var i = 0;
        var geometryTypeJoiner = new StringJoiner(",");
        var deltaGeometryTypeJoiner = new StringJoiner(",");
        for(var feature : features){
            var geometryType = GEOMETRY_TYPES.get(feature.geometry().getGeometryType());
            geometryTypes[i] = geometryType;
            var deltaGeometryType = geometryType - previousGeometryType;
            if(i < 25){
                geometryTypeJoiner.add(String.valueOf(geometryTypes[i]));
                deltaGeometryTypeJoiner.add(String.valueOf(deltaGeometryType));
            }
            deltaGeometryTypes[i++] = deltaGeometryType;
            previousGeometryType = geometryType;
        }

        try{
            var rleV1EncodedGeometryTypes = IntegerCompression.orcRleEncodingV1(geometryTypes);
            var rleV2EncodedGeometryTypes = IntegerCompression.orcRleEncodingV2(geometryTypes);
            var rleV1ByteEncodedGeometryTypes = IntegerCompression.orcRleByteEncodingV1(toByteArray(geometryTypes));
            var rleV1EncodedDeltaGeometryTypes = IntegerCompression.orcRleEncodingV1(deltaGeometryTypes);
            var rleV2EncodedDeltaGeometryTypes = IntegerCompression.orcRleEncodingV2(deltaGeometryTypes);
            var geometryIntTypes = Arrays.stream(geometryTypes).mapToInt(j -> (int) j).toArray();
            var deltaGeometryIntTypes = Arrays.stream(deltaGeometryTypes).mapToInt(j -> (int) j).toArray();
            var parquetDeltaEncodedGeometryTypes = IntegerCompression.parquetDeltaEncoding(geometryIntTypes);
            var parquetRLEEncodedGeometryTypes = IntegerCompression.parquetRLEBitpackingHybridEncoding(geometryIntTypes);
            var parquetDeltaEncodedDeltaGeometryTypes = IntegerCompression.parquetDeltaEncoding(deltaGeometryIntTypes);
            var parquetRLEEncodedDeltaGeometryTypes = IntegerCompression.parquetRLEBitpackingHybridEncoding(deltaGeometryIntTypes);

            //var num = Arrays.stream(geometryIntTypes).filter(a -> a != 1).boxed().collect(Collectors.toList());

            System.out.println(name + " -----------------------------------------");
            System.out.println("Num Types: " +  geometryTypes.length);
            System.out.println("Values: " + geometryTypeJoiner.toString());
            System.out.println("Delta Values: " + deltaGeometryTypeJoiner.toString());
            System.out.println("RLE V1: " + rleV1EncodedGeometryTypes.length);
            System.out.println("RLE V1 Byte: " + rleV1ByteEncodedGeometryTypes.length);
            System.out.println("RLE V2: " + rleV2EncodedGeometryTypes.length);
            System.out.println("RLE V1 Delta : " + rleV1EncodedDeltaGeometryTypes.length);
            System.out.println("RLE V2 Delta : " + rleV2EncodedDeltaGeometryTypes.length);
            System.out.println("Parquet Delta: " + parquetDeltaEncodedGeometryTypes.length);
            System.out.println("Parquet RLE Bitpacking: " + parquetRLEEncodedGeometryTypes.length);
            System.out.println("Parquet  Delta Delta : " + parquetDeltaEncodedDeltaGeometryTypes.length);
            System.out.println("Parquet RLE Bitpacking Delta: " + parquetRLEEncodedDeltaGeometryTypes.length);
        }
        catch(Exception e){
            System.out.println(e);
        }
    }

    private static byte[] toByteArray(long[] longArray) {
        var arr = new byte[longArray.length];
        var i = 0;
        for (long value : longArray) {
            arr[i++] = (byte)value;
        }
        return arr;
    }

    private static void analyzeIds(Layer layer) throws IOException {
        var name = layer.name();
        long previousId = 0;
        var features = layer.features();
        var ids = new long[features.size()];
        var deltaIds = new long[features.size()];
        var i = 0;
        var idJoiner = new StringJoiner(",");
        var deltaIdJoiner = new StringJoiner(",");
        for(var feature : features){
            var id = feature.id();
            ids[i] = id;
            var deltaId = id - previousId;
            if(i < 25){
                idJoiner.add(String.valueOf(id));
                deltaIdJoiner.add(String.valueOf(deltaId));
            }
            deltaIds[i++] = deltaId;
            previousId = id;
        }

        var rleV1EncodedIds = IntegerCompression.orcRleEncodingV1(ids);
        var rleV2EncodedIds = IntegerCompression.orcRleEncodingV2(ids);
        var rleV1EncodedDeltaIds = IntegerCompression.orcRleEncodingV1(deltaIds);
        var rleV2EncodedDeltaIds = IntegerCompression.orcRleEncodingV2(deltaIds);

        System.out.println(name + " -----------------------------------------");
        System.out.println("Num Ids: " +  ids.length);
        System.out.println("Values: " + idJoiner.toString());
        System.out.println("Delta Values: " + deltaIdJoiner.toString());
        System.out.println("RLE V1: " + rleV1EncodedIds.length);
        System.out.println("RLE V2: " + rleV2EncodedIds.length);
        System.out.println("RLE V1 Delta : " + rleV1EncodedDeltaIds.length);
        System.out.println("RLE V2 Delta : " + rleV2EncodedDeltaIds.length);

        if(name.equals("transportation")){
            var intIds = Arrays.stream(ids).mapToInt(j -> (int) j).toArray();
            var intDeltaIds = Arrays.stream(deltaIds).mapToInt(j -> (int) j).toArray();
            var parquetDeltaEncodedIds = IntegerCompression.parquetDeltaEncoding(intIds);
            var parquetRLEEncodedIds = IntegerCompression.parquetRLEBitpackingHybridEncoding(intIds);
            var parquetDeltaEncodedDeltaIds = IntegerCompression.parquetDeltaEncoding(intDeltaIds);
            var parquetRLEEncodedDeltaIds = IntegerCompression.parquetRLEBitpackingHybridEncoding(intDeltaIds);
            System.out.println("Parquet Delta: " + parquetDeltaEncodedIds.length);
            System.out.println("Parquet RLE Bitpacking: " + parquetRLEEncodedIds.length);
            System.out.println("Parquet  Delta Delta : " + parquetDeltaEncodedDeltaIds.length);
            System.out.println("Parquet RLE Bitpacking Delta: " + parquetRLEEncodedDeltaIds.length);
        }
    }

    private static List<Layer> parseMvt() throws SQLException, IOException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        var connection = DriverManager.getConnection("jdbc:sqlite:C:\\mapdata\\europe.mbtiles");
        var stmt = connection .createStatement();
        ResultSet rs = stmt.executeQuery( "SELECT tile_data FROM tiles WHERE tile_column = 16 AND tile_row = 21 AND zoom_level = 5;");
        //ResultSet rs = stmt.executeQuery( "SELECT tile_data FROM tiles WHERE tile_column = 16 AND tile_row = 20 AND zoom_level = 5;");
        rs.next();
        var blob = rs.getBytes("tile_data");
        rs.close();
        stmt.close();
        connection.close();

        var inputStream = new ByteArrayInputStream(blob);
        var gZIPInputStream = new GZIPInputStream(inputStream);
        var mvtTile = gZIPInputStream.readAllBytes();
        var result = MvtReader.loadMvt(
                new ByteArrayInputStream(mvtTile),
                MvtEvaluation.createGeometryFactory(),
                new TagKeyValueMapConverter(false, "id"));
        final var mvtLayers = result.getLayers();

        var layers = new ArrayList<Layer>();
        for(var layer : mvtLayers){
            var name = layer.getName();
            var mvtFeatures = layer.getGeometries();
            var features = new ArrayList<Feature>();
            for(var mvtFeature : mvtFeatures){
                //var geometryType = mvtFeature.getGeometryType();
                var properties = ((LinkedHashMap)mvtFeature.getUserData());
                var id = (long)properties.get(ID_KEY);
                properties.remove(ID_KEY);
                var feature = new Feature(id, mvtFeature, properties);
                features.add(feature);
            }

            layers.add(new Layer(name, features));
        }

        return layers;
    }

    private static GeometryFactory createGeometryFactory() {
        final PrecisionModel precisionModel = new PrecisionModel();
        final PackedCoordinateSequenceFactory coordinateSequenceFactory =
                new PackedCoordinateSequenceFactory(PackedCoordinateSequenceFactory.DOUBLE);
        return new GeometryFactory(precisionModel, 0, coordinateSequenceFactory);
    }

}
