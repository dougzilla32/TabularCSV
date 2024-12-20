import Testing
import TabularCSV
import TabularData

struct Person: Codable {
    let name: String
    let age: Int
    let height: Double
}

struct PersonRow: KeyedCodable {
    typealias CodingKeysType = CodingKeys
    
    let name: String
    let age: Int
    let height: Double
    
    enum CodingKeys: String, CodingKey, CaseIterable {
        case name = "Name"
        case age = "Age"
        case height = "Height"
    }
}

let PersonHeader = ["Name", "Age", "Height"]
let PersonCSVHeader = "Name,Age,Height\n"
let PersonCSVRows = "Alice,23,5.6\nBob,25,6.0\nCharlie,27,5.9\n"
let PersonCSV = PersonCSVHeader + PersonCSVRows

@Test func testPerson() async throws {
    do {
        let rows = try TabularCSVReader().read(Person.self, header: PersonHeader, csvData: PersonCSV.data(using: .utf8)!)
        let string = String(data: try TabularCSVWriter().csvRepresentation(rows, header: PersonHeader), encoding: .utf8)!
        #expect(string == PersonCSV)
    }
    
    do {
        let rows = try TabularCSVReader().read(PersonRow.self, csvData: PersonCSV.data(using: .utf8)!)
        let string = String(data: try TabularCSVWriter().csvRepresentation(rows), encoding: .utf8)!
        #expect(string == PersonCSV)
    }
}

@Test func testPersonNoHeader() async throws {
    let rows = try TabularCSVReader().read(Person.self, header: nil, csvData: PersonCSVRows.data(using: .utf8)!)
    let string = String(data: try TabularCSVWriter().csvRepresentation(rows, header: nil), encoding: .utf8)!
    print(string)
    #expect(string == PersonCSVRows)
}

@Test func testKeyedCodablePerson() async throws {
    let rows = try TabularCSVReader().read(PersonRow.self, csvData: PersonCSV.data(using: .utf8)!)
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
