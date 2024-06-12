import { FastPFOR } from '../../../../src/encodings/fastpfor/index';
import { arraycopy } from '../../../../src/encodings/fastpfor/util';

const FastPFOR_Raw_Test1: Uint32Array = new Uint32Array([ 187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ,187114314, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 20, 8, 8, 8, 8, 8, 8, 8,4 ]);
const FastPFOR_Raw_Test2: Uint32Array = new Uint32Array([ 1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ,1871143144, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 7984, 4, 4, 4, 4, 4, 4, 4,4 ]);
const FastPFOR_Raw_Test3: Uint32Array = new Uint32Array([-100, -99, -98, -97, -96, -95, -94, -93, -92, -91, -90, -89, -88, -87, -86, -85, -84, -83, -82, -81, -80, -79, -78, -77, -76, -75, -74, -73, -72, -71, -70, -69, -68, -67, -66, -65, -64, -63, -62, -61, -60, -59, -58, -57, -56, -55, -54, -53, -52, -51, -50, -49, -48, -47, -46, -45, -44, -43, -42, -41, -40, -39, -38, -37, -36, -35, -34, -33, -32, -31, -30, -29, -28, -27, -26, -25, -24, -23, -22, -21, -20, -19, -18, -17, -16, -15, -14, -13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]);

/*
 * Note: compressed data was generated using encodeFastPfor128, but 
 * outputting int[] rather than byte[]
 */
const FastPFOR_Compressed_Test1: Uint32Array = new Uint32Array([256, 41, 277094666, -1977546686, 554189328, 138547362, -1575975903, 277094664, -2078209502, 554189328, 138547338, 1108386337, 277094664, -2078209886, 554312208, 138547332, 1108380193, 279060744, -2078209982, 554213904, 170004612, 1108378657, 277487880, -1574893502, 554189328, 144838788, 571507745, 277094666, -1977546686, 554189328, 138547362, -1575975903, 277094664, -2078209502, 554189328, 138547338, 1108386337, 277094664, -2078209886, 554312208, 138547332, 1108380193, 16, 1838341, 1346119700, -1601406876, -253966156, 4194304, 13, -1923532518, 1313254556, -1423498410, -925527151, 1692691145, 447902261, -1668458183, 1447970476, -1851054301, 1427, -2071426936, -652458422, -2004318072, -2004318072, -2003531640, -2004318072, 8685704, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
const FastPFOR_Compressed_Test2: Uint32Array = new Uint32Array([512, 49, 613566752, 153391681, 306783378, 546457892, 1092915785, -1844894574, 605178148, 1226871369, -1841224558, 613550372, 1227100745, -1840701294, 613564708, 1227133449, -1840700398, 613566752, 153391681, 306783378, 546457892, 1092915785, -1844894574, 605178148, 1226871369, -1841224558, 613550372, 1227100745, -1840701294, 613564708, 1227133449, -1840700398, 613566752, 153391681, 306783378, 546457892, 1092915785, -1844894574, 605178148, 1226871369, -1841224558, 613550372, 1227100745, -1840701294, 613564708, 1227133449, -1840700398, 613566752, 153391681, 306783378, 58, 2038275, 673125387, 1346845747, 2020566107, -1600680829, -926960469, -253240109, 521798651, 588779268, 1262499628, 1936219988, -1685026948, -1011306588, -337586228, 65524, 134217728, 52, 1844505629, 486539326, 1047392492, -333643776, 4091376, -252961536, 15981, 1844505629, 486539326, 1047392492, -333643776, 4091376, -252961536, 15981, 1844505629, 486539326, 1047392492, -333643776, 4091376, -252961536, 15981, 1844505629, 486539326, 1047392492, -333643776, 4091376, -252961536, 15981, 1844505629, 486539326, 1047392492, -333643776, 4091376, -252961536, 15981, 1844505629, 486539326, 1047392492, -333643776, 4091376, -252961536, 15981, 1844505629, 486539326, 1047392492, 0, -2071690108, -2071690108, 2082292072, -2071690106, -2071690108, 813991044, -2071690050, -2071690108, 490825860, -2071689604, -2071690108, -2071690108, -2071675344, -2071690108, 1097368708, -2071561187, -2071690108, -2071690108, -2067910524, -2071690108, 1753515140, -2038686399, -2071690108, -2071690108, -1104116604, -2071690108, -2071690108, 2082292072, -2071690106, -2071690108, 813991044, -2071690050, -2071690108, 132, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
const FastPFOR_Compressed_Test3: Uint32Array = new Uint32Array([0, 2139062044, 2139037071, 2132709247, 529497983, -1887469697, 2139062048, 2139038095, 2132971391, 596606847, -1887469697, 2139062052, 2139039119, 2133233535, 663715711, -1887469697, 2139062056, 2139040143, 2133495679, 730824575, -1887469697, 2139062060, 2139041167, 2133757823, 797933439, -1887469697, 2139062064, 2139042191, 2134019967, 865042303, -1887469697, 2139062068, 2139043215, 2134282111, 932151167, -1887469697, 2139062072, 2139044239, 2134544255, 999260031, -1887469697, 2139062076, 2139045263, 2134806399, 1066368895, -1887469697, 2139062080, 2139046287, 2135068543, 1133477759, -1887469697, 2139062084, 2139047311, 2135330687, 1200586623, -1887469697, 2139062088, 2139048335, 2135592831, 1267695487, -1887469697, 2139062092, 2139049359, 2135854975, 1334804351, -1887469697, 2139062096, 2139050383, 2136117119, 1401913215, -1887469697, 2139062100, 2139051407, 2136379263, 1469022079, -1887469697, 2139062104, 2139052431, 2136641407, 1536130943, -1887469697, 2139062108, 2139053455, 2136903551, 1603239807, -1887469697, 2139062112, 2139054479, 2137165695, 1670348671, -1887469697, 2139062116, 2139055503, 2137427839, 1737457535, -1887469697, 2139062120, 2139056527, 2137689983, 1804566399, -1887469697, 2139062124, 2139057551, 2137952127, 1871675263, -1887469697, 2139062128, 2139058575, 2138214271, 1938784127, -1887469697, 2139062132, 2139059599, 2138476415, 2005892991, -1887469697, 2139062136, 2139060623, 2138738559, 2073001855, -1887469697, 2139062140, 2139061647, 2139000703, 2140110719, -1887469697, -2088599168, -2021227132, -1953855096, -1886483060, -1819111024, -1751738988, -1684366952, -1616994916, -1549622880, -1482250844, -1414878808, -1347506772, -1280134736, -1212762700, -1145390664, -1078018628, -1010646592, -943274556, -875902520, -808530484, -741158448, -673786412, -606414376, -539042340, -471670304, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

const testFastPforDecompress = (input: Uint32Array, expectedOutput: Uint32Array) => {
  const core = FastPFOR.default();

  const outModel = core.uncompress({
    input: input,
    inpos: 0,
    output: new Uint32Array(input.length),
    outpos: 0,
  });

  const expectedCompressed: Uint32Array = new Uint32Array(outModel.outpos);
  arraycopy(new Uint32Array(expectedOutput), 0, expectedCompressed, 0, outModel.outpos);

  const actualCompressed = new Uint32Array(outModel.outpos);
  arraycopy(outModel.output, 0, actualCompressed, 0, outModel.outpos);

  expect(actualCompressed).toEqual(expectedCompressed);
}

describe("FastPFor", () => {
  it("FastPFOR decompress (Test 1)", async () => {
    testFastPforDecompress(FastPFOR_Compressed_Test1, FastPFOR_Raw_Test1);
  });

  it("FastPFOR decompress (Test 2)", async () => {
    testFastPforDecompress(FastPFOR_Compressed_Test2, FastPFOR_Raw_Test2);
  });

  it("FastPFOR decompress (Test 3)", async () => {
    testFastPforDecompress(FastPFOR_Compressed_Test3, FastPFOR_Raw_Test3);
  });
});
