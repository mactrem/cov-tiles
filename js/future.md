# Future work on the Javascript decoder

The Javascript decoder in this repo was written in June, 2024.

It is a direct port of the minimal code needed from the reference implementation in Java in order to decode an MLT tile.

The only focus so far in development has been on correctness and no effort yet has gone into performance optimization.

Due to how the MLT specification is designed, both its flexibility and features, major opportunities exist to improve performance, as listed below:

1. Advanced encodings

- Performance estimate: 20-30 ops/s
- Effort level: moderate (days)

One of the most novel aspects of the MLT specification is efficiency gained in the combination of column-oriented data with lightweight encodings tailored to the data type. To take advantage of this combo the MLT spec has support for what we refer to as "advanced encodings", but are mainly FastPFor and FSST.

But currently the JS decoder has not yet attempted to support these such that the decoding performance benefits have not been realized and the input tiles are not yet as small as they could be.

Work on this is not yet planned, but it is tracked at https://github.com/maplibre/maplibre-tile-spec/issues/222.

1. Lazy geometry decoding

- Performance estimate: 50-70 ops/s
- Effort level: moderate (days)

Another novel aspect of the MLT specification is how feature-rich its geometry storage structure is. One of the features of its storage structure allows for on-demand (aka lazy) decoding of geometries. This allows a parser to iterate all features in a layer and only pay the cost of materializing geometries if needed and right when needed.

But the JS decoder does not yet support this feature. Work on this is not yet planned, but it is tracked at https://github.com/maplibre/maplibre-tile-spec/issues/224

1. Reducing memory allocations

- Performance estimate: 10-20 ops/s
- Effort level: easy-moderate (days)

A large amount of array allocation is needed by the current JS decoder to translate the current column-oriented design of an MLT into the row-oriented format expected by MapLibre. So, we can achieve very easy performance improvements by optimizating this array allocation in obvious ways like pre-allocating when the final size is know and using TypedArrays over Array when allocation speed of the former is more effecient the the often slower access is not critical.

But the bigger picture issue here is that too much time spent optimizing this may not be required because a bigger performance win awaits the developer who can effecitvely refactor MapLibre to work directly off of the column-oriented data. This would allow MapLibre to efficiently access data within a column without allocating the entire column of data in order to assemble a fully materialized feature. For example, the current MapLibre code expects fully materialize features with all their properties available in the expression evaluation/filtering path. It could be a big but beneficial change to explore MapLibre gaining the ability to work off of a new feature API that exposes the colum-oriented data efficiently.

1. Optimizing text decoding

- Performance estimate: 5-10 ops/s
- Effort level: easy (hours)

The JS decoder needs to handle utf-8 encoded strings stored for feature properties. It uses the JS built in [TextDecoder](https://developer.mozilla.org/en-US/docs/Web/API/TextDecoder) which is available in Node.js and Browsers. This decoder is know to be slow for short strings under 10 or so characters (which can be common) and relatively fast for long strings. So, we can speed up short string decoding by implementing our own utf8 decoder than has very little initialization overhead and efficient parsing.

1. Pre-tessellation of polygons

- Performance estimate: 100-500 ops/s
- Effort level: difficult (months)

To render areas in GL Maplibre needs to convert polygons into triangles, a process called tessellation. Internally Maplibre-gl-js does this with https://github.com/mapbox/earcut and MapLibre-native uses the C++ port at https://github.com/mapbox/earcut.hpp.

When Mapbox GL was initially launched the best of breed triangulation library used was too slow and unreliable to be production viable. So Mapbox wrote a from-scrath implementation in order to make Mapbox GL viable as a emerging library. And as users starting trying to render heavier and heavier polygon data Mapbox engineers spent hours over many years returning to earcut to hunt for additional optimizations. The Mapbox Vector Tile Spec v2 version was largerly driven by a need to improve earcut performance. Because the v1 specification and v1 implemenations did not provide explicit guarantees of polygon validity this required earcut to do extensive computational geometry operations to detect polygon holes. After the v2 spec was launched and all major tilesources were producing v2 tiles earcut was updated to be more efficient. But even after all these years of effort, tessellation with earcut is still often the top bottleneck in MapLibre by far for styles containing polygon features of any complexity.

Therefore pre-tessellation is of major research interest. It would involve:

 - Designing an efficient storage encoding for tesselated data such that the computational expense can be done once by the server
 - Updating MapLibre to be able to either cope without the original geometries or reconstruct them as needed
 - Updating MapLibre to be able to push the pre-tesselated data as efficiently as possible to the GPU

To unlock this in the spec, much investigations would be needed. See the discussion of this topic in the [bench/readme.md](bench/readme.md) and the open issue: https://github.com/maplibre/maplibre-tile-spec/issues/223. This feature could unlock a massive performance improvement to MapLibre rendering if designed well. Designing it well will require careful examination of MapLibre internals.