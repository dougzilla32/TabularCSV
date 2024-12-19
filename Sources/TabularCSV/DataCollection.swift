//
//  RowCollection.swift
//  SweepMap
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

protocol TypedRow: Collection {
    subscript<T>(position: Int, type: T.Type) -> T? { get }
    
    var count: Int { get }
}

extension DataFrame.Row: TypedRow { }

extension Array: TypedRow where Element == String? {
    subscript<T>(position: Int, type: T.Type) -> T? {
        self[position] as? T
    }
}

protocol DataDecoder: Decoder {
    func nextString(forKey key: CodingKey?) throws -> String

    func nextStringIfPresent() -> String?
}

extension Int {
    var atRow: String {
        self >= 0 ? " at row \(self+1)" : ""
    }
}

final class RowCollection<Row: TypedRow> {
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

    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoding: DataDecoder) throws -> T {
        return try options.decode(type, forKey: key, rowNumber: rowNumber, decoding: decoding)
    }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, decoding: DataDecoder) throws -> T? {
        return try options.decodeIfPresent(type, rowNumber: rowNumber, decoding: decoding)
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

final class ColumnCollection {
    let header: [String]
    let numRows: Int
    var columns: [AnyColumn] = []
    private let options: WritingOptions
    var columnIndex = 0
    
    init(header: [String], numRows: Int, options: WritingOptions) {
        self.header = header
        self.numRows = numRows
        self.options = options
    }
    
    func encode<T>(_ value: T) {
        if columns.count < columnIndex {
            columns.append(Column<T>(name: header[columnIndex], capacity: numRows).eraseToAnyColumn())
        }
        columns[columnIndex].append(value)
        columnIndex += 1
    }
    
    func encode<T: Encodable>(_ type: T.Type, value: T, rowNumber: Int, encoding: Encoder) throws {
        try options.encode(type, value: value, rowNumber: rowNumber, encoding: encoding)
    }

    func encodeIfPresent<T: Encodable>(_ type: T.Type, value: T, rowNumber: Int, encoding: Encoder) throws {
        try options.encodeIfPresent(type, value: value, rowNumber: rowNumber, encoding: encoding)
    }
}
