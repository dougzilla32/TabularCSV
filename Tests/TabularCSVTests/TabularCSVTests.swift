import Testing
import TabularCSV
import TabularData
import CodableCSV

// MARK: Person

struct Person: Codable {
    let name: String
    let age: Int
    let height: Double
    @YesNo var tall: Bool
    @Nationality var nationality: String
}

struct PersonWithoutHeight: Codable {
    let name: String
    let age: Int
    @YesNo var tall: Bool
    @Nationality var nationality: String
}

@propertyWrapper
public struct Nationality: CodableNilAsEmptyString {
    public let wrappedValue: String
    public init(wrappedValue: String) { self.wrappedValue = wrappedValue }

    public static func decode(string: String) -> String? {
        switch string {
        case "US": "United States"
        case "UK": "United Kingdom"
        case "": "Canada"
        default: nil
        }
    }
        
    public static func encode(_ value: String) -> String {
        switch value {
        case "United States": "US"
        case "United Kingdom": "UK"
        case "Canada": ""
        default: value
        }
    }
}

let PersonCSVHeader = "name,age,height,tall,nationality\n"
let PersonCSVRows = """
    Alice,23,5.6,yes,UK
    Bob,25,6.0,yes,US
    Charlie,27,5.3,no,US
    """ + "\n"
let PersonCSV = PersonCSVHeader + PersonCSVRows

enum ColumnOperation {
    case remove
    case clear
}

func modifyColumn(from csv: String, column: Int, operation: ColumnOperation, hasHeaderRow: Bool = true) -> String {
    let rows = csv.split(separator: "\n") // Split the CSV into rows
    let updatedRows = rows.enumerated().map { index, row in
        let columns = row.split(separator: ",", omittingEmptySubsequences: false) // Split the row into columns
        guard column >= 0 && column < columns.count else { return String(row) } // If column index is out of range, keep the row unchanged

        switch operation {
        case .remove:
            return columns.enumerated()
                .filter { $0.offset != column } // Exclude the specified column
                .map { String($0.element) } // Convert Substring to String
                .joined(separator: ",") // Join columns
        case .clear:
            // Skip clearing for the header row if `hasHeaderRow` is true
            if hasHeaderRow && index == 0 {
                return String(row)
            }
            return columns.enumerated()
                .map { $0.offset == column ? "" : String($0.element) } // Set the specified column to an empty string
                .joined(separator: ",") // Join columns
        }
    }
    return updatedRows.joined(separator: "\n") + "\n" // Join rows back into a CSV
}

func decodeEncode<T: Codable>(
    _ type: T.Type,
    input: String,
    hasHeaderRow: Bool = true,
    output: String? = nil,
    encodeWithHeader: Bool = true,
    includesHeader: Bool = true,
    overrideDecodeHeader: [String]? = nil,
    overrideEncodeHeader: [String]? = nil,
    expectedException: String? = nil,
    readingOptions: ReadingOptions = .init(),
    writingOptions: WritingOptions = .init()) throws
{
    do {
        let inputData = input.data(using: .utf8)!
        let decoded = try TabularDecoder(options: readingOptions).read([T].self, csvData: inputData, hasHeaderRow: hasHeaderRow, overrideHeader: overrideDecodeHeader)
        let encoded = try TabularEncoder(options: writingOptions).csvRepresentation(
            decoded.rows,
            includesHeader: includesHeader,
            overrideHeader: overrideEncodeHeader ?? (encodeWithHeader ? decoded.header : nil))
        let expected = output ?? input
        let actual = String(data: encoded, encoding: .utf8)!
        #expect(expected == actual)
        #expect(expectedException == nil)
    } catch let decodingError as DataDecodingError {
        if let expectedException {
            #expect(expectedException == decodingError.description)
        } else {
            throw decodingError
        }
    } catch let encodingError as DataEncodingError {
        if let expectedException {
            #expect(expectedException == encodingError.description)
        } else {
            throw encodingError
        }
    } catch let readingError as CSVReadingError {
        if let expectedException {
            #expect(expectedException == readingError.description)
        } else {
            throw readingError
        }
    }
}

@Test func testPerson() async throws {
    try decodeEncode(Person.self, input: PersonCSV)
}

@Test func testPersonNoHeader() async throws {
    try decodeEncode(
        Person.self,
        input: PersonCSVRows,
        hasHeaderRow: false,
        output: PersonCSV,
        encodeWithHeader: false,
        includesHeader: true
    )

    try decodeEncode(
        Person.self,
        input: PersonCSVRows,
        hasHeaderRow: false,
        output: PersonCSVRows,
        encodeWithHeader: false,
        includesHeader: false
    )
}

@Test func testDecodeNil() async throws {
    let nilCSV = "name,age,height,tall,nationality\nAlice,23,5.6,yes,\nBob,25,6.0,yes,US\nCarl,27,5.3,no,\n"
    try decodeEncode(
        Person.self,
        input: nilCSV
    )
}

@Test func testRepeatKeys() async throws {
    struct RepeatPerson: Codable {
        let name: String
        let age: Int
        let height: Double
        @YesNo var tall: Bool
        @Nationality var nationality: String
        
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            _ = try container.decodeNil(forKey: .name)
            self.name = try container.decode(String.self, forKey: .name)
            _ = try container.decodeNil(forKey: .age)
            self.age = try container.decode(Int.self, forKey: .age)
            _ = try container.decodeNil(forKey: .height)
            self.height = try container.decode(Double.self, forKey: .height)
            _ = try container.decodeNil(forKey: .tall)
            self._tall = try container.decode(YesNo.self, forKey: .tall)
            _ = try container.decodeNil(forKey: .nationality)
            self._nationality = try container.decode(Nationality.self, forKey: .nationality)
            _ = try container.decode(Nationality.self, forKey: .nationality)
        }
        
        enum CodingKeys: String, CodingKey {
            case name
            case age
            case height
            case tall
            case nationality
        }
    }
    
    try decodeEncode(
        RepeatPerson.self,
        input: PersonCSV)
}

@Test func testDecodeNilWithBool() async throws {
    struct DecodeNilWithBool: Codable {
        let name: String
        let child: Bool
        
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            _ = try container.decodeNil(forKey: .name)
            self.name = try container.decode(String.self, forKey: .name)
            if try container.decodeNil(forKey: .child) {
                child = false
            } else {
                self.child = try container.decode(Bool.self, forKey: .child)
            }
        }
        
        enum CodingKeys: String, CodingKey {
            case name
            case child
        }
    }
    
    try decodeEncode(
        DecodeNilWithBool.self,
        input: "name,child\nAlice,\nBob,true\nCarl,false\n",
        output: "name,child\nAlice,false\nBob,true\nCarl,false\n"
    )
}

@propertyWrapper
struct America: CodableString {
    public let wrappedValue: String
    public init(wrappedValue: String) { self.wrappedValue = wrappedValue }

    public static func decode(string: String) -> String? {
        switch string {
        case "US": "United States"
        default: nil
        }
    }
        
    public static func encode(_ value: String) -> String {
        switch value {
        case "United States": "US"
        default: value
        }
    }
}

@Test func testDecodeConditionals() async throws {
    struct TallAmericans: Codable {
        let name: String
        @YesNo var tall: Bool
        @America var america: String
        
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            
            if try container.decodeNil(forKey: .name) {
                name = ""
            } else {
                name = try container.decode(String.self, forKey: .name)
            }
            
            if try container.decodeNil(forKey: .tall) {
                _tall = YesNo(wrappedValue: false)
            } else {
                _tall = try container.decode(YesNo.self, forKey: .tall)
            }
            
            if try container.decodeNil(forKey: .america) {
                _america = America(wrappedValue: "")
            } else {
                _america = try container.decode(America.self, forKey: .america)
            }
        }
        
        enum CodingKeys: String, CodingKey {
            case name
            case tall
            case america
        }
    }
        
    try decodeEncode(
        TallAmericans.self,
        input: "name,tall,america\n,,\nAlice,yes,\nBob,,US\n",
        output: "name,tall,america\n,no,\nAlice,yes,\nBob,no,US\n")
}

@Test func testDecodeMapConditionals() async throws {
    struct TallAmericans: Codable {
        let name: String
        @YesNo var tall: Bool
        @America var america: String
        
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            
            if try container.decodeNil(forKey: .name)
                || container.decodeNil(forKey: .tall)
                || container.decodeNil(forKey: .america)
            {
                name = ""
                _tall = YesNo(wrappedValue: false)
                _america = America(wrappedValue: "")
            } else {
                name = try container.decode(String.self, forKey: .name)
                _tall = try container.decode(YesNo.self, forKey: .tall)
                _america = try container.decode(America.self, forKey: .america)
            }            
        }
        
        enum CodingKeys: String, CodingKey {
            case name
            case tall
            case america
        }
    }
        
    try decodeEncode(
        TallAmericans.self,
        input: "name,tall,america\n,,\nAlice,yes,\nBob,,US\nChuck,yes,US\n",
        output: "name,tall,america\n,no,\n,no,\n,no,\nChuck,yes,US\n")
}

// Test if decodeNil(forKey:) is called before decode(type, forKey:) with a different key
@Test func testMismatchedDecodeNil() async throws {
    struct TallAmericans: Codable {
        let name: String
        @YesNo var tall: Bool
        @America var america: String
        
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            
            if try container.decodeNil(forKey: .name) {
                name = ""
            } else {
                name = try container.decode(String.self, forKey: .name)
            }
            
            if try container.decodeNil(forKey: .tall) {
                _america = America(wrappedValue: "")
            } else {
                _america = try container.decode(America.self, forKey: .america)
            }
            
            if try container.decodeNil(forKey: .america) {
                _tall = YesNo(wrappedValue: false)
            } else {
                _tall = try container.decode(YesNo.self, forKey: .tall)
            }
        }
        
        enum CodingKeys: String, CodingKey {
            case name
            case tall
            case america
        }
    }
        
    try decodeEncode(
        TallAmericans.self,
        input: "name,tall,america\nAlice,yes,\nBob,,US\n",
        expectedException: "Value of type 'String' not available for key 'america' at row 2.")
}


struct Content: Codable {
    let content: [Element]

    enum Element: Codable {
        case header(Header)
        case paragraph(Paragraph)
        case footer(Footer)

        struct Header: Codable {
            let title: String
            let level: Int
        }

        struct Paragraph: Codable {
            let text: String
        }

        struct Footer: Codable {
            let style: String
            let copyright: String
        }

        struct BaseContent: Codable {
            let type: ContentType

            enum ContentType: String, Codable {
                case header
                case paragraph
                case footer
            }

            enum CodingKeys: String, CodingKey {
                case type
            }
        }

        init(from decoder: Decoder) throws {
            let baseContainer = try decoder.container(keyedBy: BaseContent.CodingKeys.self)
            let type = try baseContainer.decode(BaseContent.ContentType.self, forKey: .type)

            let container = try decoder.singleValueContainer()

            switch type {
            case .header:
                self = .header(try container.decode(Header.self))
            case .paragraph:
                self = .paragraph(try container.decode(Paragraph.self))
            case .footer:
                self = .footer(try container.decode(Footer.self))
            }
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.singleValueContainer()

            switch self {
            case .header(let header):
                try container.encode(header)
            case .paragraph(let paragraph):
                try container.encode(paragraph)
            case .footer(let footer):
                try container.encode(footer)
            }
        }
    }
}

//@Test func singleValueDecoding() async throws {
//    try decodeEncode(Content.self, input: "name,child\nAlice,\nBob,true\nCarl,false\n")
//}

// Test a branch in decoding, when using decodeNil(forKey:) followed by decode(type, forKey:)
// Need a new option for decoding with a map instead of a permutation
// Need to fix TabularDecoder with DataFrame to reorder the rows first, and then not use the permutation for every row
@Test func testConditionalDecoding() async throws {
    
}

@Test func testRepeatKeysInvalidSequence() async throws {
    struct RepeatPerson: Codable {
        let name: String
        let age: Int
        let height: Double
        @YesNo var tall: Bool
        @Nationality var nationality: String
        
        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            _ = try container.decodeNil(forKey: .name)
            self.name = try container.decode(String.self, forKey: .name)
            _ = try container.decodeNil(forKey: .age)
            self.age = try container.decode(Int.self, forKey: .age)
            _ = try container.decodeNil(forKey: .height)
            self.height = try container.decode(Double.self, forKey: .height)
            _ = try container.decodeNil(forKey: .nationality)
            self._tall = try container.decode(YesNo.self, forKey: .tall)
            _ = try container.decodeNil(forKey: .nationality)
            self._nationality = try container.decode(Nationality.self, forKey: .nationality)
            _ = try container.decode(Nationality.self, forKey: .nationality)
        }
        
        enum CodingKeys: String, CodingKey {
            case name
            case age
            case height
            case tall
            case nationality
        }
    }
    
    try decodeEncode(
        RepeatPerson.self,
        input: PersonCSV)
}

@Test func testPersonWithoutHeight() async throws {
    try decodeEncode(
        PersonWithoutHeight.self,
        input: PersonCSV,
        output: modifyColumn(from: PersonCSV, column: 2, operation: .clear)
    )

    try decodeEncode(
        PersonWithoutHeight.self,
        input: PersonCSV,
        output: modifyColumn(from: PersonCSV, column: 2, operation: .remove),
        encodeWithHeader: false
    )
}

@Test func testInvalidPerson() async throws {
    let invalidPersonCSV = PersonCSV + "\nage,name\n123,John"

    try decodeEncode(
        Person.self,
        input: invalidPersonCSV,
        expectedException: "Wrong number of columns at row 4. Expected 5 but found 2.")
}

@Test func testCSVWriter() async throws {
    var columns: [AnyColumn] = []
    columns.append(Column<Bool>(name: "Bool", capacity: 2).eraseToAnyColumn())
    columns.append(Column<String>(name: "String", capacity: 2).eraseToAnyColumn())
    columns[0].append(true)
    columns[0].append(false)
    columns[1].append("Hi")
    columns[1].append("there")
    
    let data = try DataFrame(columns: columns).csvRepresentation(options: CSVWritingOptions(includesHeader: true))
    let string = String(data: data, encoding: .utf8)!
    #expect(string == "Bool,String\ntrue,Hi\nfalse,there\n")
}

@Test func testEmptyCSV() async throws {
    let emptyCSV = "name,age,height,tall,nationality\n"
    try decodeEncode(Person.self, input: emptyCSV, output: emptyCSV)
}

@Test func testSingleRowCSV() async throws {
    let singleRowCSV = "name,age,height,tall,nationality\nAlice,30,5.5,no,US\n"
    try decodeEncode(Person.self, input: singleRowCSV, output: singleRowCSV)
}

@Test func testPartialRowMissingOptionalValues() async throws {
    let partialRowCSV = "name,age,height,tall,nationality\nAlice,30,,yes,US\n"
    try decodeEncode(
        PersonWithoutHeight.self,
        input: partialRowCSV,
        output: "name,age,height,tall,nationality\nAlice,30,,yes,US\n"
    )
}

@Test func testCustomColumnOrder() async throws {
    let customOrderCSV = "age,name,height,nationality,tall\n25,Bob,6.0,US,yes\n"
    try decodeEncode(Person.self, input: customOrderCSV)
}

@Test func testExtraColumns() async throws {
    let extraColumnsCSV = "name,age,height,tall,nationality,extra\nAlice,30,5.5,no,US,extraValue\n"
    try decodeEncode(
        Person.self,
        input: extraColumnsCSV,
        output: "name,age,height,tall,nationality,extra\nAlice,30,5.5,no,US,\n"
    )
}

@Test func testMissingColumnHeader() async throws {
    let missingHeaderCSV = ",age,height,tall,nationality\nAlice,30,5.5,no,US\n"
    try decodeEncode(
        Person.self,
        input: missingHeaderCSV,
        expectedException: "Value of type 'String' not available for key 'name' at row 2."
    )
}

@Test func testInvalidDataTypeInNumericColumn() async throws {
    let invalidDataTypeCSV = "name,age,height,tall,nationality\nAlice,abc,5.5,no,US\n"
    try decodeEncode(
        Person.self,
        input: invalidDataTypeCSV,
        expectedException: "Cannot decode 'abc' for key 'age' at row 2."
    )
}

@Test func testUnrecognizedEnumValue() async throws {
    let invalidEnumCSV = "name,age,height,tall,nationality\nAlice,30,5.5,no,CA\n"
    try decodeEncode(
        Person.self,
        input: invalidEnumCSV,
        expectedException: "Cannot decode 'CA' for key 'nationality' at row 2."
    )
}

@Test func testEmptyFile() async throws {
    let emptyFile = ""
    try decodeEncode(
        Person.self,
        input: emptyFile
    )
}

@Test func testIncorrectBooleanFormat() async throws {
    let invalidBooleanCSV = "name,age,height,tall,nationality\nAlice,30,5.5,YESPLEASE,US\n"
    try decodeEncode(
        Person.self,
        input: invalidBooleanCSV,
        expectedException: "Cannot decode 'YESPLEASE' for key 'tall' at row 2."
    )
}

// MARK: Pet and Cat

class Pet: Codable {
    let name: String
    let age: Int
    @YesNo var friendly: Bool
    
    enum CodingKeys: String, CodingKey {
        case name = "Name"
        case age = "Age"
        case friendly = "Friendly"
    }
}

class Cat: Pet {
    let color: String
    let longHair: Bool
    
    required init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.color = try container.decode(String.self, forKey: .color)
        self.longHair = try container.decode(Bool.self, forKey: .longHair)
        try super.init(from: decoder)
    }
    
    override func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(color, forKey: .color)
        try container.encode(longHair, forKey: .longHair)
        try super.encode(to: encoder)
    }
    
    enum CodingKeys: String, CodingKey {
        case color = "Color"
        case longHair = "Long Hair"
    }
}

let CatCSVHeader = "Name,Age,Friendly,Color,Long Hair\n"
let CatCSVRows = "Alice,2,yes,red,true\nBob,3,no,white,false\nCharlie,5,yes,black,true\n"
let CatCSV = CatCSVHeader + CatCSVRows

@Test func testCat() async throws {
    try decodeEncode(Cat.self, input: CatCSV)
}

let CatCSVHeaderScramble = "Color,Friendly,Name,Long Hair,Age\n"
let CatCSVRowsScramble = """
    red,yes,Alice,true,2
    white,no,Bob,false,3
    black,yes,Charlie,true,5
    """ + "\n"
let CatCSVScramble = CatCSVHeaderScramble + CatCSVRowsScramble

@Test func testCatScramble() async throws {
    try decodeEncode(Cat.self, input: CatCSVScramble)
}



@Test func testCatSpecialNamesAndColors() async throws {
    let specialCatCSV = """
        Name,Age,Friendly,Color,Long Hair
        Mr. Whiskers,4,yes,bright pink,false
        Princess Purr,2,no,neon green,true
        """ + "\n"
    try decodeEncode(Cat.self, input: specialCatCSV)
}

@Test func testPetNonBinaryFriendly() async throws {
    let petCSV = """
        Name,Age,Friendly
        Rex,5,yes
        Luna,3,no
        """ + "\n"
    try decodeEncode(Pet.self, input: petCSV)
}

@Test func testCatNoHeader() async throws {
    let catNoHeaderCSV = """
        red,true,Alice,2,yes
        white,false,Bob,3,no
        black,true,Charlie,5,yes
        """ + "\n"
    try decodeEncode(
        Cat.self,
        input: catNoHeaderCSV,
        hasHeaderRow: false,
        encodeWithHeader: false,
        includesHeader: false
    )
}

@Test func testCatWithExtraColumns() async throws {
    let extraColumnCSV = """
        Name,Age,Friendly,Color,Long Hair,Weight
        Alice,2,yes,red,true,4.5
        Bob,3,no,white,false,6.0
        """ + "\n"
    try decodeEncode(
        Cat.self,
        input: extraColumnCSV,
        output: modifyColumn(from: extraColumnCSV, column: 5, operation: .clear)
    )
}

@Test func testCatAlternateHeaderOrder() async throws {
    let invalidHeaderOrderCSV = """
        Age,Friendly,Name,Color,Long Hair
        2,yes,Alice,red,true
        3,no,Bob,white,false
        """ + "\n"
    try decodeEncode(
        Cat.self,
        input: invalidHeaderOrderCSV
    )
}

@Test func testCatEmptyInput() async throws {
    let emptyCSV = ""
    try decodeEncode(
        Cat.self,
        input: emptyCSV
    )
}

@Test func testInvalidPetAge() async throws {
    let invalidAgeCSV = """
        Name,Age,Friendly
        Rex,five,yes
        Luna,-1,no
        """ + "\n"
    try decodeEncode(
        Pet.self,
        input: invalidAgeCSV,
        expectedException: "Cannot decode 'five' for key 'Age' at row 2."
    )
}

@Test func testCatMissingColumns() async throws {
    let missingColumnCSV = """
        Name,Age,Friendly,Color
        Alice,2,yes,red
        Bob,3,no,white
        """ + "\n"
    try decodeEncode(
        Cat.self,
        input: missingColumnCSV,
        expectedException: "Value of type 'Bool' not available for key 'Long Hair' at row 2."
    )
}

@Test func testInvalidFriendlyValue() async throws {
    let invalidFriendlyCSV = """
        Name,Age,Friendly,Color,Long Hair
        Alice,2,maybe,red,true
        Bob,3,123,white,false
        """ + "\n"
    try decodeEncode(
        Cat.self,
        input: invalidFriendlyCSV,
        expectedException: "Cannot decode 'maybe' for key 'Friendly' at row 2."
    )
}

@Test func allTypes() async throws {
    struct AllTypes: Codable {
        let stringValue: String
        let boolValue: Bool
        let doubleValue: Double
        let floatValue: Float
        let intValue: Int
        let int32Value: Int32
        let int64Value: Int64
        let uIntValue: UInt
        let uInt32Value: UInt32
        let uInt64Value: UInt64
    }
    
    let AllTypesHeader = """
            stringValue,boolValue,doubleValue,floatValue,intValue,int32Value,int64Value,uIntValue,uInt32Value,uInt64Value
            """ + "\n"
    let AllTypesData = """
            Hello World!,true,3.14159,3.14159,42,42,42,42,42,42
            """ + "\n"
    
    try decodeEncode(
        AllTypes.self,
        input: AllTypesData,
        hasHeaderRow: false,
        includesHeader: false)

    try decodeEncode(
        AllTypes.self,
        input: AllTypesHeader + AllTypesData,
        hasHeaderRow: true,
        includesHeader: true)
}

@Test func badEncodingHeader() async throws {
    struct SomeTypes: Codable {
        let stringValue: String
        let boolValue: Bool
    }
    
    let SomeTypesData = """
            Hello World!,true
            """ + "\n"
    
    try decodeEncode(
        SomeTypes.self,
        input: SomeTypesData,
        hasHeaderRow: false,
        includesHeader: false)
}

@Test func reserveOrderEncoding() async throws {
    struct SomeTypes: Codable {
        let stringValue: String
        let boolValue: Bool
    }
    
    let SomeTypesData = """
            stringValue,boolValue
            Hello World!,true
            """ + "\n"
    let SomeTypesHeaderReverse = ["boolValue", "stringValue"]
    let SomeTypesDataReverse = """
            boolValue,stringValue
            true,Hello World!
            """ + "\n"
    
    try decodeEncode(
        SomeTypes.self,
        input: SomeTypesData,
        output: SomeTypesDataReverse,
        overrideEncodeHeader: SomeTypesHeaderReverse)
}

@Test func emptyEncoding() async throws {
    struct SomeTypes: Codable {
        let stringValue: String
        let boolValue: Bool
    }
    
    let SomeTypesHeader = """
            stringValue,boolValue
            """ + "\n"
    
    try decodeEncode(
        SomeTypes.self,
        input: SomeTypesHeader)
}

// TODO:
@Test func decodingSingleValue() async throws {
    struct SingleValue: Codable {
        let value: String

        // Custom initializer for decoding
        init(from decoder: Decoder) throws {
            let container = try decoder.singleValueContainer()
            _ = try container.decode(String.self)
            _ = try container.decode(String.self)
            self.value = try container.decode(String.self)
        }

        // Custom initializer for encoding (optional, for completeness)
        func encode(to encoder: Encoder) throws {
            var container = encoder.singleValueContainer()
            try container.encode(value)
            try container.encode(value)
            try container.encode(value)
        }
    }
    
    let SingleValueCSV = """
            value
            Hello World!
            """ + "\n"
    
    try decodeEncode(
        SingleValue.self,
        input: SingleValueCSV)

    let SingleValueCSVNoHeader = """
            Hello World!
            """ + "\n"

    try decodeEncode(
        SingleValue.self,
        input: SingleValueCSVNoHeader,
        hasHeaderRow: false,
        includesHeader: false)
}

@Test func encodingSingleValue() async throws {
}

@Test func decodingNestedUnkeyedContainer() async throws {
}

@Test func encodingNestedUnkeyedContainer() async throws {
}

@Test func decodingNestedKeyedContainer() async throws {
}

@Test func encodingNestedKeyedContainer() async throws {
}

@Test func superDecoder() async throws {
}

@Test func superEncoder() async throws {
}

@Test func superDecoderKeyed() async throws {
}

@Test func superEncoderKeyed() async throws {
}
