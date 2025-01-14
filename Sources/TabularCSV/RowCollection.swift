//
//  RowCollection.swift
//  TabularCSV
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

// MARK: Data rows and columns

public protocol DataRow {
    subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? { get }
    var count: Int { get }
    func isNil(at position: Int) -> Bool
}


public protocol DataRows: Collection where Element: DataRow { }

public protocol DataColumns {
    func type(at position: Int) -> CSVPrimitive.Type
}


// MARK: DataFrame rows and columns

extension DataFrame.Row: DataRow {
    public subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? {
        self[position, type]
    }
    
    public func isNil(at position: Int) -> Bool {
        self[position] == nil
    }
}

extension DataFrame.Rows: DataRows { }

extension Array: DataColumns where Element == AnyColumn {
    public func type(at position: Int) -> CSVPrimitive.Type {
        self[position].wrappedElementType as! CSVPrimitive.Type
    }
}


// MARK: String rows and columns

extension Array: DataRow where Element == String? {
    public subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? {
        guard let string = self[position] else { return nil }
        if options.nilEncodings.contains(string) { return nil }
        if type == Bool.self {
            if options.trueEncodings.contains(string) { return true as? T }
            if options.falseEncodings.contains(string) { return false as? T }
        }
        return T(string)
    }
    
    public func isNil(at position: Int) -> Bool {
        self[position] == nil
    }
}

extension Array: DataRows where Element == [String?] { }

public struct StringColumns: DataColumns {
    public func type(at position: Int) -> CSVPrimitive.Type {
        String.self
    }
}


struct HeaderAndType {
    let name: String
    let type: CSVType
}

public enum RowTransform {
    case none
    case permutation([Int?])
    case map([String: Int]?)
    
    init(_ permutation: [Int?]?) {
        if let permutation {
            self = .permutation(permutation)
        } else {
            self = .none
        }
    }
    
    init(_ keys: [String]?) {
        if let keys {
            let mapping = Dictionary(uniqueKeysWithValues: keys.enumerated().map { ($1, $0) })
            self = .map(mapping)
        } else {
            self = .map(nil)
        }
    }
}

protocol RowCollection {
    var rowCount: Int { get }
    var rowNumber: Int { get }
    var currentRowIndex: Int { get }

    func nextRow() throws
    func nextRowIfPresent() throws -> Bool

    func decodeNil(forKey key: CodingKey?) throws -> Bool
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey?) throws -> T
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey?) throws -> T?
    func decodeNext<T: Decodable>(_ type: T.Type, forKey key: CodingKey?, decoder: Decoder) throws -> T
    func decodeNextIfPresent<T: Decodable>(_ type: T.Type, forKey key: CodingKey?, decoder: Decoder) throws -> T?
    
    func singleValueContainer()
    func singleValueDecode(isDecodeNil: Bool) throws
}

final class RowPermutation<Rows: DataRows, Columns: DataColumns>: RowCollection {
    private var table: Table<Rows, Columns>
    private var permutation: [Int?]?

    init(rows: Rows, columns: Columns, permutation: [Int?]?, options: ReadingOptions) {
        self.table = Table(rows: rows, columns: columns, options: options)
        self.permutation = permutation
    }
    
    var rowCount: Int { table.rowCount }
    var rowNumber: Int { table.rowNumber }
    var currentRowIndex: Int { table.currentRowIndex }

    func nextRow() throws { try table.nextRow() }
    func nextRowIfPresent() -> Bool { table.nextRowIfPresent() }

    func decodeNil(forKey key: CodingKey? = nil) throws -> Bool {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return true }

        let index = permutation?[table.currentColumnIndex] ?? table.currentColumnIndex
        let isNil = table.isNilValue(row: row, index: index)
        if isNil {
            table.currentColumnIndex += 1
        }
        return isNil
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = try decodeNextIfPresent(T.self, forKey: key) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: table.rowNumber)
        }
        return value
    }
    
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T? {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return nil }
        
        let index = permutation?[table.currentColumnIndex] ?? table.currentColumnIndex
        table.currentColumnIndex += 1
        return table.getValue(type, row: row, index: index)
    }
    
    func decodeNext<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        let value: T
        if let parser = table.options.parserForType(type) {
            let string = try decodeNext(String.self, forKey: key)
            value = try table.parse(type, string: string, forKey: key, parser: parser)
        } else {
            value = try table.decode(type, forKey: key, decoder: decoder)
        }
        return value
    }
    
    func decodeNextIfPresent<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T? {
        let value: T?
        if let parser = table.options.parserForType(type) {
            guard let string = try decodeNextIfPresent(String.self, forKey: key), !string.isEmpty else { return nil }
            value = try table.parse(type, string: string, parser: parser)
        } else {
            if try decodeNil(forKey: key) {
                value = nil
            } else {
                value = try table.decode(type, forKey: key, decoder: decoder)
            }
        }
        return value
    }

    func singleValueContainer() { }
    func singleValueDecode(isDecodeNil: Bool) throws { }
}

final class RowMap<Rows: DataRows, Columns: DataColumns>: RowCollection {
    private var table: Table<Rows, Columns>
    private var map: [String: Int]?

    init(rows: Rows, columns: Columns, map: [String: Int]?, options: ReadingOptions) {
        self.table = Table(rows: rows, columns: columns, options: options)
        self.map = map
    }
    
    var rowCount: Int { table.rowCount }
    var rowNumber: Int { table.rowNumber }
    var currentRowIndex: Int { table.currentRowIndex }

    func nextRow() throws {
        try table.nextRow()
    }

    func nextRowIfPresent() throws -> Bool {
        table.nextRowIfPresent()
    }
    
    func decodeNil(forKey key: CodingKey? = nil) throws -> Bool {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return true }

        let index: Int
        if let key {
            index = map?[key.stringValue] ?? table.currentColumnIndex
        } else {
            index = table.currentColumnIndex
        }

        let isNil = table.isNilValue(row: row, index: index)
        if isNil {
            table.currentColumnIndex += 1
        }
        return isNil
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = try decodeNextIfPresent(T.self, forKey: key) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: table.rowNumber)
        }
        return value
    }
    
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T? {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return nil }
        
        let index: Int
        if let key {
            index = map?[key.stringValue] ?? table.currentColumnIndex
        } else {
            index = table.currentColumnIndex
        }

        table.currentColumnIndex += 1
        return table.getValue(type, row: row, index: index)
    }
    
    func decodeNext<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        let value: T
        if let parser = table.options.parserForType(type) {
            let string = try decodeNext(String.self, forKey: key)
            value = try table.parse(type, string: string, forKey: key, parser: parser)
        } else {
            value = try table.decode(type, forKey: key, decoder: decoder)
        }
        return value
    }
    
    func decodeNextIfPresent<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T? {
        let value: T?
        if let parser = table.options.parserForType(type) {
            if let string = try decodeNextIfPresent(String.self, forKey: key) {
                value = try table.parse(type, string: string, parser: parser)
            } else {
                value = nil
            }
        } else {
            if try decodeNil(forKey: key) {
                value = nil
            } else {
                value = try table.decode(type, forKey: key, decoder: decoder)
            }
        }
        return value
    }

    func singleValueContainer() { }
    func singleValueDecode(isDecodeNil: Bool) throws { }
}

final class RowTypes<Rows: DataRows, Columns: DataColumns>: RowCollection {
    private var table: Table<Rows, Columns>
    private var map: [String: Int]?
    private(set) var headerAndTypes: [HeaderAndType] = []
    private var decodeNilKey: CodingKey?
    private var singleValueDecodingIndex: Int?
    private var usedKeys: Set<String> = []

    init(rows: Rows, columns: Columns, map: [String: Int]?, options: ReadingOptions) {
        self.table = Table(rows: rows, columns: columns, options: options)
        self.map = map
    }
    
    var rowCount: Int { table.rowCount }
    var rowNumber: Int { table.rowNumber }
    var currentRowIndex: Int { table.currentRowIndex }

    func nextRow() throws {
        try nextRowCheck()
        try table.nextRow()
    }

    func nextRowIfPresent() throws -> Bool {
        try nextRowCheck()
        return table.nextRowIfPresent()
    }
    
    private func nextRowCheck() throws {
        decodeNilKey = nil
        if singleValueDecodingIndex != nil {
            throw CSVDecodingError.singleValueDecoding(rowNumber: table.rowNumber)
        }
    }

    func decodeNil(forKey key: CodingKey? = nil) throws -> Bool {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return true }

        if let decodeNilKey {
            throw CSVDecodingError.incorrectSequence(String.self, nilKey: decodeNilKey, currentKey: key, rowNumber: table.rowNumber)
        }
        self.decodeNilKey = key

        return false
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = try decodeNextIfPresent(T.self, forKey: key) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: table.rowNumber)
        }
        return value
    }
    
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T? {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return nil }
        
        let value: T?
        switch type {
        case is Bool.Type:   value = Bool(true) as? T
        case is Double.Type: value = 0.0 as? T
        case is Float.Type:  value = 0.0 as? T
        case is any FixedWidthInteger.Type: value = 0 as? T
        case is String.Type:
            let index: Int
            if let key {
                index = map?[key.stringValue] ?? table.currentColumnIndex
            } else {
                index = table.currentColumnIndex
            }
            value = table.getValue(type, row: row, index: index) ?? ("" as? T)
        default:
            fatalError("Unsupported type for CSVPrimitive.")
        }
        
        // Sanity check for decodeNil followed by decodeNextIfPresent
        if let decodeNilKey, let key, decodeNilKey.stringValue != key.stringValue {
            throw CSVDecodingError.incorrectSequence(T.self, nilKey: decodeNilKey, currentKey: key, rowNumber: rowNumber)
        }
        self.decodeNilKey = nil

        if let key {
            usedKeys.insert(key.stringValue)
            headerAndTypes.append(.init(name: key.stringValue, type: type.csvType))
        }

        table.currentColumnIndex += 1

        return value
    }
    
    func decodeNext<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        let value: T
        if let parser = table.options.parserForType(type) {
            let string = try decodeNext(String.self, forKey: key)
            value = try table.parse(type, string: string, forKey: key, parser: parser)
        } else {
            if let key {
                headerAndTypes.append(.init(name: key.stringValue, type: .string))
            }
            value = try table.decode(type, forKey: key, decoder: decoder)
        }
        return value
    }
    
    func decodeNextIfPresent<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T? {
        let value: T?
        if let parser = table.options.parserForType(type) {
            if let string = try decodeNextIfPresent(String.self, forKey: key) {
                value = try table.parse(type, string: string, parser: parser)
            } else {
                value = nil
            }
        } else {
            if try decodeNil(forKey: key) {
                value = nil
            } else {
                if let key {
                    headerAndTypes.append(.init(name: key.stringValue, type: .string))
                }
                value = try table.decode(type, forKey: key, decoder: decoder)
            }
        }
        return value
    }

    func singleValueContainer() {
        self.singleValueDecodingIndex = table.currentColumnIndex
    }

    func singleValueDecode(isDecodeNil: Bool) throws {
        guard let singleValueDecodingIndex else {
            throw CSVDecodingError.singleValueDecoding(rowNumber: table.rowNumber)
        }
        guard singleValueDecodingIndex == table.currentColumnIndex else {
            throw CSVDecodingError.singleValueDecoding(rowNumber: table.rowNumber)
        }
        if !isDecodeNil {
            self.singleValueDecodingIndex = nil
        }
    }
}

fileprivate struct Table<Rows: DataRows, Columns: DataColumns> {
    let rowCount: Int
    private var rowsIterator: Rows.Iterator
    let columns: Columns
    let options: ReadingOptions
    var currentRow: Rows.Element?
    private(set) var rowNumber: Int
    private(set) var currentRowIndex: Int
    var currentColumnIndex: Int
    
    init(rows: Rows, columns: Columns, options: ReadingOptions) {
        self.rowCount = rows.count
        self.rowsIterator = rows.makeIterator()
        self.columns = columns
        self.options = options
        self.currentRow = nil
        self.rowNumber = options.csvReadingOptions.hasHeaderRow ? 1 : 0
        self.currentRowIndex = 0
        self.currentColumnIndex = -1
    }
    
    mutating func nextRow() throws {
        guard nextRowIfPresent() else {
            throw CSVDecodingError.isAtEnd(rowNumber: rowNumber)
        }
    }
    
    mutating func nextRowIfPresent() -> Bool {
        guard let row = rowsIterator.next() else { return false }
        
        currentRow = row
        rowNumber += 1
        currentRowIndex += 1
        currentColumnIndex = 0
        return true
    }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        do {
//                if let key {
//                    let string = try decodeNext(String.self, forKey: key)
//                    value = try T(from: try StringRowsDecoder.singleton(rows: [[string]], columns: StringColumns(), options: table.options))
//                } else {
            return try T(from: decoder)
//                }
        } catch CodableStringError.invalidFormat(let string) {
            throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        } catch let csvError as CSVDecodingError {
            throw csvError.withKey(key, rowNumber: rowNumber)
        }
    }
    
    func getValue<T: CSVPrimitive>(_ type: T.Type, row: Rows.Element, index: Int) -> T? {
        var value = row[index, type, options]
        if options.nilAsEmptyString, type == String.self, value == nil {
            value = "" as? T
        }
        return value
    }
    
    func isNilValue(row: Rows.Element, index: Int) -> Bool {
        var isNil = row.isNil(at: index)
        if isNil, options.nilAsEmptyString, columns.type(at: index) == String.self {
            isNil = false
        }
        return isNil
    }

    func parse<T>(_ type: T.Type, string: String, forKey key: CodingKey? = nil, parser: ((String) -> Any)) throws -> T {
        guard let value = parser(string) as? T else {
            throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
}
