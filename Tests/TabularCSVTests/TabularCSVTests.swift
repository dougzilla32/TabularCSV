import Testing
import TabularCSV
import TabularData
import CodableCSV

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
public struct Nationality: CodableString {
    public let wrappedValue: String
    public init(wrappedValue: String) { self.wrappedValue = wrappedValue }

    public static func decode(string: String) -> String? {
        switch string {
        case "US": "United States"
        case "UK": "United Kingdom"
        default: nil
        }
    }
        
    public static func encode(_ value: String) -> String {
        switch value {
        case "United States": "US"
        case "United Kingdom": "UK"
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

let PersonCSVRowsBlankHeight = """
    Alice,23,,yes,UK
    Bob,25,,yes,US
    Charlie,27,,no,US
    """ + "\n"
let PersonCSVBlankHeight = PersonCSVHeader + PersonCSVRowsBlankHeight

let PersonCSVHeaderNoHeight = "name,age,tall,nationality\n"
let PersonCSVRowsNoHeight = """
    Alice,23,yes,UK
    Bob,25,yes,US
    Charlie,27,no,US
    """ + "\n"
let PersonCSVNoHeight = PersonCSVHeaderNoHeight + PersonCSVRowsNoHeight


@Test func testPerson() async throws {
    let csv = try TabularCSVReader().read([Person].self, csvData: PersonCSV.data(using: .utf8)!)
    let string = String(data: try TabularCSVWriter().csvRepresentation(csv.rows, overrideHeader: csv.header), encoding: .utf8)!
    #expect(string == PersonCSV)
}

@Test func testPersonWithoutHeight() async throws {
    let csv = try TabularCSVReader().read([PersonWithoutHeight].self, csvData: PersonCSV.data(using: .utf8)!)
    let blankHeight = String(data: try TabularCSVWriter().csvRepresentation(csv.rows, overrideHeader: csv.header), encoding: .utf8)!
    #expect(blankHeight == PersonCSVBlankHeight)
    let noHeight = String(data: try TabularCSVWriter().csvRepresentation(csv.rows), encoding: .utf8)!
    #expect(noHeight == PersonCSVNoHeight)
}

@Test func testPersonNoHeader() async throws {
    let csv = try TabularCSVReader().read([Person].self, csvData: PersonCSVRows.data(using: .utf8)!, hasHeaderRow: false)
    let withHeader = String(data: try TabularCSVWriter().csvRepresentation(csv.rows), encoding: .utf8)!
    let withoutHeader = String(data: try TabularCSVWriter().csvRepresentation(csv.rows, includesHeader: false), encoding: .utf8)!
    #expect(withHeader == PersonCSV)
    #expect(withoutHeader == PersonCSVRows)
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
    let csv = try TabularCSVReader().read([Cat].self, csvData: CatCSV.data(using: .utf8)!)
    let string = String(data: try TabularCSVWriter().csvRepresentation(csv.rows, overrideHeader: csv.header), encoding: .utf8)!
    #expect(string == CatCSV)
}
