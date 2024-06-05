import * as fs from "fs";
import { MVTLayer, parseMvtTile } from "../../../src/mvtUtils";
import { CovtDecoder } from "../../../src/decoder/covtDecoder";
import mvtFixtures from "@mapbox/mvt-fixtures";
import Pbf from "pbf";
// import { TileSetMetadata as MltTilesetMetadata } from "../../../src/decoder/mltTilesetMetadata";
import { TileSetMetadata as MltTilesetMetadata } from "../../../src/decoder/mlt_tileset_metadata_pb";
import * as Path from "path";
import { toBeDeepCloseTo, toMatchCloseTo } from "jest-matcher-deep-close-to";
expect.extend({ toBeDeepCloseTo, toMatchCloseTo });

const tilesDir = "./data";

describe("CovtDecoder", () => {
    it("should decode a simple valid tile", () => {
        const tiles = getMvtFixtureTiles("017"); // Small valid MVT fixture
        const tile = tiles[0];
        const covTile = fs.readFileSync(tile.covt);

        const mltMetadataPbf = fs.readFileSync(tile.mltMetadata);
        const tilesetMetadata = MltTilesetMetadata.fromBinary(mltMetadataPbf);

        const covtDecoder = new CovtDecoder(covTile, tilesetMetadata);

        const mvtLayerNames = parseMvtTile(tile.mvt).map((l) => l.name);

        expect(covtDecoder.layerNames).toBe(mvtLayerNames);
        expect(covtDecoder.getLayerTable(mvtLayerNames[0])).toBeDefined();
    });

    it.skip("should decode Amazon based tiles", async () => {
        const tiles = getTiles(Path.join(tilesDir, "amazon"));

        for (const tile of tiles) {
            console.info(tile.covt);
            const covTile = fs.readFileSync(tile.covt);
            const mvtTile = fs.readFileSync(tile.mvt);
            const mvtLayers = parseMvtTile(mvtTile);

            const covtDecoder = new CovtDecoder(covTile, null);

            compareTiles(covtDecoder, mvtLayers, true);
        }
    });

    it.skip("should decode Bing Map based tiles", async () => {
        const tiles = getTiles(Path.join(tilesDir, "bing"));

        for (const tile of tiles) {
            console.info(tile.covt);
            // NB This tile is very slow (test times out), we skip it here for now
            if (tile.covt === "data/bing/5-16-11.covt") continue;
            const covTile = fs.readFileSync(tile.covt);
            const mvtTile = fs.readFileSync(tile.mvt);
            const mvtLayers = parseMvtTile(mvtTile);

            const covtDecoder = new CovtDecoder(covTile, null);

            /* The features of Bing tiles have no ids */
            compareTiles(covtDecoder, mvtLayers, false);
        }
    });

    it.skip("should decode OpenMapTiles schema based tiles", async () => {
        const tiles = getTiles(Path.join(tilesDir, "omt"));

        for (const tile of tiles) {
            const covTile = fs.readFileSync(tile.covt);
            const mvtTile = fs.readFileSync(tile.mvt);
            const mvtLayers = parseMvtTile(mvtTile);

            const covtDecoder = new CovtDecoder(covTile, null);

            compareTiles(covtDecoder, mvtLayers);
        }
    });
});

function mapMvtProperties(mvtProperties: Map<string, unknown>): Map<string, unknown> {
    const transformedMvtProperties = new Map<string, unknown>();
    for (const [key, value] of mvtProperties) {
        if (value === undefined) {
            continue;
        }

        /* Bing Maps tiles contain id in the feature properties and the feature id is set to 0.
         * The Java MVT decoder used for the generation of the COVTiles ignores this id while the js decoder inserts the id in the feature properties.
         * */
        if (key.includes("id")) {
            //console.info("remove id property: ", value);
            continue;
        }

        let transformedKey = key;
        if (key.includes("name_")) {
            const comps = key.split("_");
            transformedKey = `${comps[0]}:${comps[1]}`;
        }

        //TODO: get rid of BigInt
        const convertedValue = Number.isInteger(value) ? BigInt(value as number) : value;
        transformedMvtProperties.set(transformedKey, convertedValue);
    }

    return transformedMvtProperties;
}

function getMvtFixtureTiles(id: string): { mvt: Buffer; mltMetadata: string; covt: string }[] {
    const dir = Path.join(tilesDir, "mvt-fixtures", "017");
    const tiles = fs.readdirSync(dir);
    return [
        {
            covt: Path.join(dir, tiles.find((t) => t.endsWith(".mlt"))),
            mvt: mvtFixtures.get(id).buffer,
            mltMetadata: Path.join(dir, tiles.find((t) => t.endsWith(".mlt.pbf"))),
        },
    ];
}

function getTiles(dir: string): { mvt: string; covt: string }[] {
    const tiles = fs.readdirSync(dir).sort();
    const mappedTiles = [];
    for (let i = 0; i < tiles.length; i += 2) {
        mappedTiles.push({ covt: Path.join(dir, tiles[i]), mvt: Path.join(dir, tiles[i + 1]) });
    }
    return mappedTiles;
}

/*
 * Get a plain object representation of the geometry points
 *
 * Specifically remove member functions included by vector-tile
 */
function getPlainObjectGeometry(geometry) {
    if (geometry instanceof Array) {
        return geometry.map(part => getPlainObjectGeometry(part));
    }
    return {
        x: geometry.x,
        y: geometry.y,
    };
}

function compareTiles(covtDecoder: CovtDecoder, mvtLayers: MVTLayer[], compareIds = true) {
    //let mvtLayerId = 0;
    for (const covtLayerName of covtDecoder.layerNames) {
        const covtLayer = covtDecoder.getLayerTable(covtLayerName);
        //const mvtLayer = mvtLayers[mvtLayerId++];
        //Depending on the used MVT decoding library the layer can be in different order
        const mvtLayer = mvtLayers.find((l) => l.name === covtLayer.layerMetadata.name);

        //console.info(covtLayerName);

        /* the following layers are sorted based on the id in COVTiles */
        if (["building", "poi", "place"].indexOf(covtLayerName) !== -1) {
            mvtLayer.features.sort((a, b) => a.id - b.id);
        }

        Array.from(covtLayer).forEach((covtFeature, i) => {
            const { id: covtId, geometry: covtGeometry, properties: covtProperties } = covtFeature;
            const { id: mvtId, geometry: mvtGeometry, properties: mvtProperties } = mvtLayer.features[i];

            //TODO: fix id issue in place layer
            if (covtLayerName !== "place" && compareIds) {
                expect(covtId).toEqual(mvtId);
            }

            /*if (covtLayerName === "water_feature") {
                try {
                    expect(covtGeometry.format()).toEqual(mvtGeometry);
                } catch (e) {
                    console.error("error ", covtLayerName);
                }
            }*/

            expect(covtGeometry.format()).toEqual(getPlainObjectGeometry(mvtGeometry));

            const transformedMvtProperties = mapMvtProperties(mvtProperties);
            (expect(covtProperties) as any).toMatchCloseTo(transformedMvtProperties, 8);
        });
    }
}
