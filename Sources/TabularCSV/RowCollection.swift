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
    var count: Int { get }
    subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? { get }
    func getAny(at position: Int) -> Any?
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
    
    public func getAny(at position: Int) -> Any? {
        self[position]
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
    
    public func getAny(at position: Int) -> Any? {
        self[position]
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
    func deferredError(_ error: Error)
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
        let isNil = table.isNil(row: row, index: index)
        if isNil {
            table.currentColumnIndex += 1
        }
        return isNil
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = try decodeNextIfPresent(T.self, forKey: key) else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: table.rowNumber)
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
    func deferredError(_ error: Error) { table.deferredError = error }
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

        let isNil = table.isNil(row: row, index: index)
        if isNil {
            table.currentColumnIndex += 1
        }
        return isNil
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = try decodeNextIfPresent(T.self, forKey: key) else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: table.rowNumber)
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
        return table.getValueFromString(type, row: row, index: index)
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
    func deferredError(_ error: Error) { table.deferredError = error }
}

enum RowTypeError: Error {
    case nilDecodingError
}

final class RowTypes<Rows: DataRows, Columns: DataColumns>: RowCollection {
    private var table: Table<Rows, Columns>
    private var map: [String: Int]?
    private(set) var nilKeys: Set<String> = []
    private(set) var headerAndTypes: [HeaderAndType] = []
    private var decodeNilKey: CodingKey?
    private var singleValueDecodingIndex: Int?
    private var usedKeys: Set<String> = []
    private var currentKey = Stack<CodingKey>()
    
    init(rows: Rows, columns: Columns, map: [String: Int]?, nilKeys: Set<String>, options: ReadingOptions) {
        self.table = Table(rows: rows, columns: columns, options: options)
        self.map = map
        self.nilKeys = nilKeys
    }
    
    var rowCount: Int { table.rowCount }
    var rowNumber: Int { table.rowNumber }
    var currentRowIndex: Int { table.currentRowIndex }
    
    private func appendHeaderAndType(key: CodingKey, type: CSVType) throws {
        let duplicate = self.headerAndTypes.contains { $0.name == key.stringValue }
        if duplicate {
            throw DataDecodingError.duplicateSequence(String.self, nilKey: decodeNilKey ?? key, currentKey: key, rowNumber: table.rowNumber)
        }
        headerAndTypes.append(HeaderAndType(name: key.stringValue, type: type))
    }
    
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
            throw DataDecodingError.singleValueDecoding(rowNumber: table.rowNumber)
        }
    }
    
    func decodeNil(forKey key: CodingKey? = nil) throws -> Bool {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return true }
        
        if let key { currentKey.push(key) }
        defer { if key != nil { currentKey.pop() } }
        let key = currentKey.top
        
        if let decodeNilKey {
            throw DataDecodingError.incorrectNilSequence(String.self, nilKey: decodeNilKey, currentKey: key, rowNumber: table.rowNumber)
        }
        
        let isNil: Bool
        if let key, nilKeys.contains(key.stringValue) {
            isNil = true
            usedKeys.insert(key.stringValue)
            try appendHeaderAndType(key: key, type: .string)
        } else {
            isNil = false
            self.decodeNilKey = key
        }
        return isNil
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        if let key { currentKey.push(key) }
        defer { if key != nil { currentKey.pop() } }
        let key = currentKey.top
        
        let nilKeyMatch = decodeNilKeyMatch(key: key)
        
        guard let value = try decodeNextIfPresent(T.self, forKey: currentKey.top) else {
            let error = DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: table.rowNumber)
            throw nilDecodingError(nilKeyMatch, forKey: key, error: error)
        }
        return value
    }
    
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T? {
        guard let row = table.currentRow, table.currentColumnIndex < row.count else { return nil }
        
        if let key { currentKey.push(key) }
        defer { if key != nil { currentKey.pop() } }
        let key = currentKey.top
        
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
            value = table.getValue(type, row: row, index: index)
        default:
            fatalError("Unsupported type for CSVPrimitive.")
        }
        
        // Sanity check for decodeNil followed by decodeNextIfPresent
        if let decodeNilKey, let key, decodeNilKey.stringValue != key.stringValue {
            throw DataDecodingError.incorrectSequence(T.self, nilKey: decodeNilKey, currentKey: key, rowNumber: rowNumber)
        }
        self.decodeNilKey = nil
        
        if let key {
            usedKeys.insert(key.stringValue)
            try appendHeaderAndType(key: key, type: type.csvType)
        }
        
        table.currentColumnIndex += 1
        
        return value
    }
    
    func decodeNext<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        if let key { currentKey.push(key) }
        defer { if key != nil { currentKey.pop() } }
        let key = currentKey.top
        
        let nilKeyMatch = decodeNilKeyMatch(key: key)
        
        let value: T
        if let parser = table.options.parserForType(type) {
            let string = try decodeNext(String.self, forKey: key)
            do {
                value = try table.parse(type, string: string, forKey: key, parser: parser)
            } catch {
                throw nilDecodingError(nilKeyMatch, forKey: key, error: error)
            }
        } else {
            do {
                value = try table.decode(type, forKey: key, decoder: decoder)
            } catch {
                throw nilDecodingError(nilKeyMatch, forKey: key, error: error)
            }
        }
        return value
    }
    
    func decodeNextIfPresent<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T? {
        if let key { currentKey.push(key) }
        defer { if key != nil { currentKey.pop() } }
        let key = currentKey.top
        
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
                do {
                    value = try table.decode(type, forKey: key, decoder: decoder)
                } catch {
                    throw nilDecodingError(true, forKey: key, error: error)
                }
            }
        }
        return value
    }
    
    private func decodeNilKeyMatch(key: CodingKey?) -> Bool {
        let isMatch: Bool
        if let decodeNilKey, let key, decodeNilKey.stringValue == key.stringValue {
            isMatch = true
        } else {
            isMatch = false
        }
        return isMatch
    }
    
    private func nilDecodingError(_ nilKeyMatch: Bool, forKey key: CodingKey?, error: Error) -> Error {
        if nilKeyMatch, let key {
            nilKeys.insert(key.stringValue)
            return RowTypeError.nilDecodingError
        } else {
            return error
        }
    }
    
    func singleValueContainer() {
        self.singleValueDecodingIndex = table.currentColumnIndex
    }
    
    func singleValueDecode(isDecodeNil: Bool) throws {
        guard let singleValueDecodingIndex else {
            throw DataDecodingError.singleValueDecoding(rowNumber: table.rowNumber)
        }
        guard singleValueDecodingIndex == table.currentColumnIndex else {
            throw DataDecodingError.singleValueDecoding(rowNumber: table.rowNumber)
        }
        if !isDecodeNil {
            self.singleValueDecodingIndex = nil
        }
    }
    
    func deferredError(_ error: Error) {
        table.deferredError = error
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
    var deferredError: Error?
    
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
            throw DataDecodingError.isAtEnd(rowNumber: rowNumber)
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
            let value = try T(from: decoder)
            if let deferredError {
                throw deferredError
            }
            return value
        } catch CodableStringError.invalidFormat(let string) {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        } catch let csvError as DataDecodingError {
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
    
    func getValueFromString<T: CSVPrimitive>(_ type: T.Type, row: Rows.Element, index: Int) -> T? {
        let value: T?
        let anyValue = row.getAny(at: index)
        if let anyValue {
            value = (anyValue as? T) ?? T(String(describing: anyValue))
        } else if options.nilAsEmptyString, type == String.self {
            value = "" as? T
        } else {
            value = nil
        }
        return value
    }
    
    func isNil(row: Rows.Element, index: Int) -> Bool {
        var isNil = (row.getAny(at: index) == nil)
        if isNil, options.nilAsEmptyString, columns.type(at: index) == String.self {
            isNil = false
        }
        return isNil
    }

    func parse<T>(_ type: T.Type, string: String, forKey key: CodingKey? = nil, parser: ((String) -> Any)) throws -> T {
        guard let value = parser(string) as? T else {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
}

public struct Stack<T> {
    fileprivate var array = [T]()
    
    public var isEmpty: Bool {
        return array.isEmpty
    }
    
    public var count: Int {
        return array.count
    }
    
    public mutating func push(_ element: T) {
        array.append(element)
    }
    
    @discardableResult
    public mutating func pop() -> T? {
        return array.popLast()
    }
    
    public var top: T? {
        return array.last
    }
}

extension Stack: Sequence {
    public func makeIterator() -> AnyIterator<T> {
        var curr = self
        return AnyIterator {
            return curr.pop()
        }
    }
}
