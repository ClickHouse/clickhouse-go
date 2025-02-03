package column

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractEnumNamedValues(t *testing.T) {
	tests := []struct {
		name           string
		chType         Type
		expectedType   string
		expectedValues map[int]string
		isNotValid     bool
	}{
		{
			name:         "Enum8",
			chType:       "Enum8('a'=1,'b'=2)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum16",
			chType:       "Enum16('a'=1,'b'=2)",
			expectedType: "Enum16",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 with comma in value",
			chType:       "Enum8('a'=1,'b'=2,'c,d'=3)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
				3: "c,d",
			},
		},
		{
			name:         "Enum8 with spaces",
			chType:       "Enum8('a' = 1, 'b' = 2)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 without indexes",
			chType:       "Enum8('a','b')",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 with a first index only",
			chType:       "Enum8('a'=1,'b')",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "b",
			},
		},
		{
			name:         "Enum8 with a last index only",
			chType:       "Enum8('a','b'=5)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				5: "b",
			},
		},
		{
			name:         "Enum8 with a first index only higher than 1",
			chType:       "Enum8('a'=5,'b')",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				5: "a",
				6: "b",
			},
		},
		{
			name:         "Enum8 with index with spaces",
			chType:       "Enum8( 'a' , 'b' = 5 )",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				5: "b",
			},
		},
		{
			name:         "Enum8 with escaped quotes",
			chType:       `Enum8('a\'b'=1)`,
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a'b",
			},
		},
		{
			name:       "Enum8 with invalid index",
			chType:     "Enum8('a'=1,'b'=256)",
			isNotValid: true,
		},
		{
			name:       "Enum8 with invalid non-integer index",
			chType:     "Enum8('a'=1,'b'='c')",
			isNotValid: true,
		},
		{
			name:       "Empty Enum8",
			chType:     "Enum8()",
			isNotValid: true,
		},
		{
			name:       "Empty Enum8 without brackets",
			chType:     "Enum8",
			isNotValid: true,
		},
		{
			name:         "Enum8 with empty key",
			chType:       "Enum8('a'=1, ''=2)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				1: "a",
				2: "",
			},
		},
		{
			name:         "Enum8 from negative to zero keys",
			chType:       "Enum8('a'=-1, 'b'=0)",
			expectedType: "Enum8",
			expectedValues: map[int]string{
				-1: "a",
				0:  "b",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualType, actualValues, actualIndexes, valid := extractEnumNamedValues(tt.chType)

			if tt.isNotValid {
				assert.False(t, valid, "%s is valid enum", tt.chType)
				return
			}

			actualValuesMap := make(map[int]string)
			for i, v := range actualValues {
				actualValuesMap[actualIndexes[i]] = v
			}

			assert.Equal(t, tt.expectedType, actualType)
			assert.Equal(t, tt.expectedValues, actualValuesMap)

			assert.True(t, valid, "%s is not valid enum", tt.chType)
		})
	}
}

func TestEnumValuesBoundsChecks(t *testing.T) {
	tests := []struct {
		name       string
		enumType   string
		validEnums []int
	}{
		{
			name:       "Simple enum range",
			enumType:   "Enum8('-2'=-2,'-1'=-1,'0'=0,'1'=1,'2'=2)",
			validEnums: createValidEnumsRange(-2, 2),
		},
		{
			name:       "Full enum range",
			enumType:   "Enum8('-128'=-128,'-127'=-127,'-126'=-126,'-125'=-125,'-124'=-124,'-123'=-123,'-122'=-122,'-121'=-121,'-120'=-120,'-119'=-119,'-118'=-118,'-117'=-117,'-116'=-116,'-115'=-115,'-114'=-114,'-113'=-113,'-112'=-112,'-111'=-111,'-110'=-110,'-109'=-109,'-108'=-108,'-107'=-107,'-106'=-106,'-105'=-105,'-104'=-104,'-103'=-103,'-102'=-102,'-101'=-101,'-100'=-100,'-99'=-99,'-98'=-98,'-97'=-97,'-96'=-96,'-95'=-95,'-94'=-94,'-93'=-93,'-92'=-92,'-91'=-91,'-90'=-90,'-89'=-89,'-88'=-88,'-87'=-87,'-86'=-86,'-85'=-85,'-84'=-84,'-83'=-83,'-82'=-82,'-81'=-81,'-80'=-80,'-79'=-79,'-78'=-78,'-77'=-77,'-76'=-76,'-75'=-75,'-74'=-74,'-73'=-73,'-72'=-72,'-71'=-71,'-70'=-70,'-69'=-69,'-68'=-68,'-67'=-67,'-66'=-66,'-65'=-65,'-64'=-64,'-63'=-63,'-62'=-62,'-61'=-61,'-60'=-60,'-59'=-59,'-58'=-58,'-57'=-57,'-56'=-56,'-55'=-55,'-54'=-54,'-53'=-53,'-52'=-52,'-51'=-51,'-50'=-50,'-49'=-49,'-48'=-48,'-47'=-47,'-46'=-46,'-45'=-45,'-44'=-44,'-43'=-43,'-42'=-42,'-41'=-41,'-40'=-40,'-39'=-39,'-38'=-38,'-37'=-37,'-36'=-36,'-35'=-35,'-34'=-34,'-33'=-33,'-32'=-32,'-31'=-31,'-30'=-30,'-29'=-29,'-28'=-28,'-27'=-27,'-26'=-26,'-25'=-25,'-24'=-24,'-23'=-23,'-22'=-22,'-21'=-21,'-20'=-20,'-19'=-19,'-18'=-18,'-17'=-17,'-16'=-16,'-15'=-15,'-14'=-14,'-13'=-13,'-12'=-12,'-11'=-11,'-10'=-10,'-9'=-9,'-8'=-8,'-7'=-7,'-6'=-6,'-5'=-5,'-4'=-4,'-3'=-3,'-2'=-2,'-1'=-1,'0'=0,'1'=1,'2'=2,'3'=3,'4'=4,'5'=5,'6'=6,'7'=7,'8'=8,'9'=9,'10'=10,'11'=11,'12'=12,'13'=13,'14'=14,'15'=15,'16'=16,'17'=17,'18'=18,'19'=19,'20'=20,'21'=21,'22'=22,'23'=23,'24'=24,'25'=25,'26'=26,'27'=27,'28'=28,'29'=29,'30'=30,'31'=31,'32'=32,'33'=33,'34'=34,'35'=35,'36'=36,'37'=37,'38'=38,'39'=39,'40'=40,'41'=41,'42'=42,'43'=43,'44'=44,'45'=45,'46'=46,'47'=47,'48'=48,'49'=49,'50'=50,'51'=51,'52'=52,'53'=53,'54'=54,'55'=55,'56'=56,'57'=57,'58'=58,'59'=59,'60'=60,'61'=61,'62'=62,'63'=63,'64'=64,'65'=65,'66'=66,'67'=67,'68'=68,'69'=69,'70'=70,'71'=71,'72'=72,'73'=73,'74'=74,'75'=75,'76'=76,'77'=77,'78'=78,'79'=79,'80'=80,'81'=81,'82'=82,'83'=83,'84'=84,'85'=85,'86'=86,'87'=87,'88'=88,'89'=89,'90'=90,'91'=91,'92'=92,'93'=93,'94'=94,'95'=95,'96'=96,'97'=97,'98'=98,'99'=99,'100'=100,'101'=101,'102'=102,'103'=103,'104'=104,'105'=105,'106'=106,'107'=107,'108'=108,'109'=109,'110'=110,'111'=111,'112'=112,'113'=113,'114'=114,'115'=115,'116'=116,'117'=117,'118'=118,'119'=119,'120'=120,'121'=121,'122'=122,'123'=123,'124'=124,'125'=125,'126'=126,'127'=127)",
			validEnums: createValidEnumsRange(-128, 127),
		},
		{
			name:       "Enum range with gaps",
			enumType:   "Enum8('-10'=-10,'-5'=-5,'0'=0,'1'=1,'5'=5,'10'=10)",
			validEnums: []int{-10, -5, 0, 1, 5, 10},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, err := Enum(Type(tt.enumType), tt.name)
			assert.NoError(t, err)

			// Try appending the full enum8 range. If the value is in the validEnums slice it should not error
			for i := -128; i < 128; i++ {
				valid := e.AppendRow(i)

				if slices.Contains(tt.validEnums, i) {
					assert.NoError(t, valid)
				} else {
					assert.Error(t, valid)
				}
			}
		})
	}
}

func createValidEnumsRange(min, max int) []int {
	resultRange := make([]int, 0, max-min+1)
	for i := min; i <= max; i++ {
		resultRange = append(resultRange, i)
	}
	return resultRange
}
