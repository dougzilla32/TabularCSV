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

struct PersonKeyed: KeyedCodable {
    typealias CodingKeysType = CodingKeys
    
    let name: String
    let age: Int
    let height: Double
    @YesNo var tall: Bool
    @Nationality var nationality: String
    
    enum CodingKeys: String, CodingKey, CaseIterable {
        case name = "Name"
        case age = "Age"
        case height = "Height"
        case tall = "Tall"
        case nationality = "Nationality"
    }
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

let PersonHeader = ["Name", "Age", "Height", "Tall", "Nationality"]
let PersonCSVHeader = "Name,Age,Height,Tall,Nationality\n"
let PersonCSVRows = "Alice,23,5.6,yes,UK\nBob,25,6.0,yes,US\nCharlie,27,5.3,no,US\n"
let PersonCSV = PersonCSVHeader + PersonCSVRows

@Test func testPerson() async throws {
    do {
        let rows = try TabularCSVReader().read([Person].self, header: PersonHeader, csvData: PersonCSV.data(using: .utf8)!)
        print(rows.count)
        let string = String(data: try TabularCSVWriter().csvRepresentation(rows, header: PersonHeader), encoding: .utf8)!
        #expect(string == PersonCSV)
    }
    
    do {
        let rows = try TabularCSVReader().read([PersonKeyed].self, csvData: PersonCSV.data(using: .utf8)!)
        let string = String(data: try TabularCSVWriter().csvRepresentation(rows), encoding: .utf8)!
        #expect(string == PersonCSV)
    }
}

@Test func testPersonWithoutHeight() async throws {
    let rows = try TabularCSVReader().read([PersonWithoutHeight].self, header: PersonHeader, csvData: PersonCSV.data(using: .utf8)!)
    let string = String(data: try TabularCSVWriter().csvRepresentation(rows, header: PersonHeader), encoding: .utf8)!
    #expect(string == PersonCSV)
}

@Test func testPersonNoHeader() async throws {
    let rows = try TabularCSVReader().read([Person].self, header: nil, csvData: PersonCSVRows.data(using: .utf8)!)
    let string = String(data: try TabularCSVWriter().csvRepresentation(rows, header: nil), encoding: .utf8)!
    print(string)
    #expect(string == PersonCSVRows)
}

@Test func testKeyedCodablePerson() async throws {
    let rows = try TabularCSVReader().read([PersonKeyed].self, csvData: PersonCSV.data(using: .utf8)!)
    let string = String(data: try TabularCSVWriter().csvRepresentation(rows), encoding: .utf8)!
    #expect(string == PersonCSV)
}

@Test func testCSVWriter() async throws {
    var columns: [AnyColumn] = []
    columns.append(Column<Bool>(name: "Bool Column", capacity: 2).eraseToAnyColumn())
    columns.append(Column<String>(name: "String Column", capacity: 2).eraseToAnyColumn())
    columns[0].append(true)
    columns[0].append(false)
    columns[1].append("Hi")
    columns[1].append("there")
    
    let data = try DataFrame(columns: columns).csvRepresentation(options: CSVWritingOptions(includesHeader: true))
    let string = String(data: data, encoding: .utf8)!
    print("STRING \(string)")
}

@Test func testCodableCSV() async throws {
    let rows = try CSVDecoder().decode([Person].self, from: PersonCSV.data(using: .utf8)!)
}


class Pet: Codable {
    let name: String
    let age: Int
    @YesNo var friendly: Bool
}

class Cat: Pet {
    let color: String

    required init(from decoder: any Decoder) throws {
        self.color = try decoder.singleValueContainer().decode(String.self)
        try super.init(from: decoder)
    }
}

class PetKeyed: KeyedCodable {
    typealias CodingKeysType = CodingKeys
    
    let name: String
    let age: Int
    @YesNo var friendly: Bool
    
    enum CodingKeys: String, CodingKey, CaseIterable {
        case name = "Name"
        case age = "Age"
        case friendly = "Friendly"
    }
}

class CatKeyed: Pet, KeyedCodable {
    typealias CodingKeysType = CodingKeys
    
    let color: String
    
    required init(from decoder: any Decoder) throws {
        self.color = try decoder.singleValueContainer().decode(String.self)
        try super.init(from: decoder)
    }
    
    enum CodingKeys: String, CodingKey, CaseIterable {
        case color = "Color"
    }
}

let CatHeader = ["Color", "Name", "Age", "Friendly"]
let CatCSVHeader = "Color,Name,Age,Friendly\n"
let CatCSVRows = "red,Alice,2,yes\nwhite,Bob,3,no\nblack,Charlie,5,yes\n"
let CatCSV = CatCSVHeader + CatCSVRows

@Test func testCat() async throws {
    do {
        let rows = try TabularCSVReader().read([Cat].self, header: CatHeader, csvData: CatCSV.data(using: .utf8)!)
        print(rows.count)
        let string = String(data: try TabularCSVWriter().csvRepresentation(rows, header: CatHeader), encoding: .utf8)!
        #expect(string == CatCSV)
    }
    
    do {
        let rows = try TabularCSVReader().read([CatKeyed].self, csvData: CatCSV.data(using: .utf8)!)
        let string = String(data: try TabularCSVWriter().csvRepresentation(rows), encoding: .utf8)!
        #expect(string == CatCSV)
    }
}
