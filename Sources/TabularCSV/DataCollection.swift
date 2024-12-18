//
//  DataCollection.swift
//  SweepMap
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import TabularData

protocol TypedCollection: Collection {
    subscript<T>(position: Int, type: T.Type) -> T? { get }
}

extension DataFrame.Row: TypedCollection { }

extension Array: TypedCollection where Element == String? {
    subscript<T>(position: Int, type: T.Type) -> T? {
        self[position] as? T
    }
}

final class DataCollection<Row: TypedCollection> {
    let row: Row
    let rowNumber: Int
    private let rowMapping: [Int?]?
    private let options: ReadingOptions
    private(set) var currentIndex: Int = 0

    init(row: Row, rowNumber: Int, rowMapping: [Int?]?, options: ReadingOptions) {
        self.row = row
        self.rowNumber = rowNumber
        self.rowMapping = rowMapping
        self.options = options
    }
    
    func nextValue<T>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = nextValueIfPresent(T.self) else {
            var codingPath = [CodingKey]()
            var description = "No value found"
            if let key = key {
                codingPath.append(key)
                description += " for key \"\(key.stringValue)\""
            }
            description += "\(rowNumber.atRow)."

            throw CSVDecodingError.valueNotFound(
                String.self,
                DecodingError.Context(codingPath: codingPath, debugDescription: description))
        }
        return value
    }
    
    func nextValueIfPresent<T>(_ type: T.Type) -> T? {
        guard currentIndex < row.count else { return nil }
        var value = row[rowMapping?[currentIndex] ?? currentIndex, type]
        if type == String.self, value == nil {
            value = "" as? T
        }
        currentIndex += 1
        return value
    }

    func nextString(forKey key: CodingKey? = nil) throws -> String {
        return try nextValue(String.self, forKey: key)
    }

    func nextStringIfPresent() -> String? {
        return nextValueIfPresent(String.self)
    }

    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        let string = try nextString(forKey: key)
        return try options.decode(type, string: string, rowNumber: rowNumber)
    }

    func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? {
        guard let string = nextStringIfPresent(), !string.isEmpty else {
            return nil
        }
        return try options.decodeIfPresent(type, string: string, rowNumber: rowNumber)
    }
    
    func decode<T: LosslessStringConvertible>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = try decodeIfPresent(type) else {
            var codingPath = [CodingKey]()
            var description = "Cannot decode value"
            if let key = key {
                codingPath.append(key)
                description += " for key \"\(key.stringValue)\""
            }
            description += "\(rowNumber.atRow)."

            throw CSVDecodingError.dataCorrupted(
                DecodingError.Context(codingPath: codingPath, debugDescription: description))
        }
        return value
    }

    func decodeIfPresent<T: LosslessStringConvertible>(_ type: T.Type) throws -> T? {
        guard let string = nextStringIfPresent(), !string.isEmpty else {
            return nil
        }
        return T(string)
    }
}
