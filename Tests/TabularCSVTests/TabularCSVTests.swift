import Testing
@testable import TabularCSV

import Foundation

@Test func testSimpleCSV() async throws {
    struct Person: DecodableRow {
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
        
    let csvString = "Name,Age,Height\nAlice,23,5.6\nBob,25,6.0\nCharlie,27,5.9\n"
    let reader = TabularCSVReader<Person, Person>(type: Person.self)
    let rows = try reader.read(csvData: csvString.data(using: .utf8)!)
}
