//
//  TypedDecoder.swift
//  TabularCSV
//
//  Created by Doug on 12/5/24.
//  Based partially on: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

import TabularData

public typealias DataFrameDecoder = TypedDecoder<DataFrame.Rows, [AnyColumn]>

public typealias StringDecoder = TypedDecoder<[[String?]], StringColumns>

public struct TypedDecoder<Rows: DataRows, Columns: DataColumns> {
    private let options: ReadingOptions
    
    public init(options: ReadingOptions) { self.options = options }
    
    public func decode<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        columns: Columns,
        header: [String]) throws -> T
    {
        let headerIndicies = Dictionary(uniqueKeysWithValues: header.enumerated().map { ($1, $0) })
        let decoder = DataUnkeyedDecoder(header: headerIndicies, rows: rows, columns: columns, options: options, introspector: nil)
        return try T(from: decoder)
    }

    func decodeTypes<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        columns: Columns,
        header: [String]?) throws -> (value: T, fields: OrderedSet<CSVField>)
    {
        let headerIndicies: [String: Int]?
        if let header {
            headerIndicies = Dictionary(uniqueKeysWithValues: header.enumerated().map { ($1, $0) })
        } else {
            headerIndicies = nil
        }

        let introspector = DataDecodingIntrospector()
        var result: (value: T, fields: OrderedSet<CSVField>)?

        repeat {
            let decoder = DataUnkeyedDecoder(header: headerIndicies, rows: rows, columns: columns, options: options, introspector: introspector)
            do {
                let value = try T(from: decoder)
                result = (value: value, fields: introspector.fields)
            } catch {
                if !introspector.checkNilKeyMatch() {
                    throw error
                }
            }
            introspector.resetLoop()
        } while result == nil

        return result!
    }
}

final class DataDecodingIntrospector {
    private(set) var fields = OrderedSet<CSVField>()
    private var nilKeys: Set<String> = []
    private var decodeNilKey: CodingKey? = nil
    private var decodeNilKeyMatch: CodingKey? = nil
    var currentColumnIndex = 0
    var currentKey: CodingKey?
    
    func decodeNil(forKey key: CodingKey, rowNumber: Int) throws -> Bool {
        let isNil: Bool
        if nilKeys.contains(key.stringValue) {
            isNil = true
            try addCSVField(key: key, type: String.self, rowNumber: rowNumber)
            currentColumnIndex += 1
        } else {
            isNil = false
            self.decodeNilKey = key
        }
        return isNil
    }
    
    func addCSVField<T: CSVPrimitive>(key: CodingKey?, type: T.Type, rowNumber: Int) throws {
        if let decodeNilKey, decodeNilKey.stringValue == key?.stringValue {
            decodeNilKeyMatch = decodeNilKey
        } else {
            decodeNilKeyMatch = nil
        }
        
        fields.add(.init(name: (key ?? currentKey)?.stringValue ?? "Column 0", type: type.csvType))
        currentColumnIndex += 1
    }
    
    func checkNilKeyMatch() -> Bool {
        let match: Bool
        if let decodeNilKeyMatch {
            nilKeys.insert(decodeNilKeyMatch.stringValue)
            match = true
        } else {
            match = false
        }
        fields.removeAll()
        decodeNilKey = nil
        decodeNilKeyMatch = nil
        return match
    }
    
    func resetDecodeNilKey() {
        self.decodeNilKey = nil
    }
    
    func resetLoop() {
        fields.removeAll()
        decodeNilKey = nil
        decodeNilKeyMatch = nil
        currentColumnIndex = 0
    }
    
    static func primitivePlaceholder<T: CSVPrimitive>(_ type: T.Type) -> T {
        switch T.self {
        case is Bool.Type:
            return true as! T
        case is Double.Type:
            return 0.0 as! T
        case is Float.Type:
            return Float(0.0) as! T
        case is Int.Type:
            return 0 as! T
        case is Int32.Type:
            return Int32(0) as! T
        case is Int64.Type:
            return Int64(0) as! T
        case is UInt.Type:
            return UInt(0) as! T
        case is UInt32.Type:
            return UInt32(0) as! T
        case is UInt64.Type:
            return UInt64(0) as! T
        default:
            if #available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *) {
                switch T.self {
                case is Int128.Type:
                    return Int128(0) as! T
                case is UInt128.Type:
                    return UInt128(0) as! T
                default:
                    fatalError("Unsupported type for CSVPrimitive.")
                }
            } else {
                fatalError("Unsupported type for CSVPrimitive.")
            }
        }
    }
}

fileprivate struct IntrospectedDataKeyedDecoding<Key: CodingKey, Row: DataRow, Columns: DataColumns>: KeyedDecodingContainerProtocol {
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { header?[key.stringValue] != nil }
    
    private let header: [String: Int]?
    private let row: Row
    private let columns: Columns
    private let rowNumber: Int
    private let options: ReadingOptions
    private let introspector: DataDecodingIntrospector
    private let unkeyedDecoder: Decoder

    init(header: [String: Int]?, row: Row, columns: Columns, rowNumber: Int, options: ReadingOptions, introspector: DataDecodingIntrospector, unkeyedDecoder: Decoder) {
        self.header = header
        self.row = row
        self.columns = columns
        self.rowNumber = rowNumber
        self.options = options
        self.introspector = introspector
        self.unkeyedDecoder = unkeyedDecoder
    }
    
    func index(forKey key: CodingKey) -> Int? {
        let index: Int
        if let header {
            guard let mappedIndex = header[key.stringValue] else {
                return nil
            }
            index = mappedIndex
        } else {
            index = introspector.currentColumnIndex
        }
        return index
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
        if let header {
            guard header[key.stringValue] != nil else { return true }
        }
        return try introspector.decodeNil(forKey: key, rowNumber: rowNumber)
    }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try decodePrim(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try decodeString(forKey: key) }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try decodePrim(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try decodePrim(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try decodePrim(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try decodePrim(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try decodePrim(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try decodePrim(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try decodePrim(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: Int128.Type,  forKey key: Key) throws -> Int128  { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try decodePrim(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: UInt128.Type, forKey key: Key) throws -> UInt128 { try decodePrim(type, forKey: key) }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        guard let value = try decodeValue(type, forKey: key, isRequired: true) else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { try decodeStringIfPresent(forKey: key) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { try decodePrimIfPresent(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decodeIfPresent(_ type: Int128.Type,  forKey key: Key) throws -> Int128?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { try decodePrimIfPresent(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decodeIfPresent(_ type: UInt128.Type, forKey key: Key) throws -> UInt128? { try decodePrimIfPresent(type, forKey: key) }
    
    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        return try decodeValue(type, forKey: key, isRequired: false)
    }

    private func decodeValue<T: Decodable>(_ type: T.Type, forKey key: Key, isRequired: Bool) throws -> T? {
        if let parser = options.parserForType(type) {
            guard let string = try isRequired ? decodeString(forKey: key) : decodeStringIfPresent(forKey: key), !string.isEmpty || !isRequired else {
                return nil
            }
            
            guard let parsedValue = parser(string) else {
                throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
            }
            return parsedValue
        }
        
        guard let index = index(forKey: key) else {
            return nil
        }

        if !isRequired && row.isNil(at: index) && !options.nilAsEmptyString {
            return nil
        }
        
        let decoder = DataSingleValueDecoder(index: index, key: key, row: row, rowNumber: rowNumber, options: options, introspector: introspector)
        do {
            return try T(from: decoder)
        } catch CodableStringError.invalidFormat(let string) {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
    }

    private func decodePrim<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey) throws -> T {
        guard index(forKey: key) != nil else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        try introspector.addCSVField(key: key, type: type, rowNumber: rowNumber)
        return DataDecodingIntrospector.primitivePlaceholder(type)
    }
    
    private func decodePrimIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey) throws -> T? {
        guard index(forKey: key) != nil else {
            return nil
        }
        try introspector.addCSVField(key: key, type: type, rowNumber: rowNumber)
        return DataDecodingIntrospector.primitivePlaceholder(type)
    }
    
    private func decodeString(forKey key: CodingKey) throws -> String {
        guard let index = index(forKey: key) else {
            throw DataDecodingError.valueNotFound(String.self, forKey: key, rowNumber: rowNumber)
        }
        try introspector.addCSVField(key: key, type: String.self, rowNumber: rowNumber)
        var value = row[index, String.self, options]
        if value == nil, options.nilAsEmptyString {
            value = ""
        }
        guard let value else {
            throw DataDecodingError.valueNotFound(String.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    private func decodeStringIfPresent(forKey key: CodingKey) throws -> String? {
        guard let index = index(forKey: key)  else {
            return nil
        }
        try introspector.addCSVField(key: key, type: String.self, rowNumber: rowNumber)
        return row[index, String.self, options] ?? (options.nilAsEmptyString ? "" : nil)
    }
    
    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) throws -> KeyedDecodingContainer<NestedKey>
    {
        throw DataDecodingError.nestedContainer(forKey: key, rowNumber: rowNumber)
    }
    
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DataDecodingError.nestedContainer(forKey: key, rowNumber: rowNumber)
    }
    
    func superDecoder() throws -> any Decoder {
        unkeyedDecoder
    }
    
    func superDecoder(forKey key: Key) throws -> any Decoder {
        throw DataDecodingError.superDecoder(forKey: key, rowNumber: rowNumber)
    }
}

fileprivate struct DataKeyedDecoding<Key: CodingKey, Row: DataRow, Columns: DataColumns>: KeyedDecodingContainerProtocol {
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { header[key.stringValue] != nil }
    
    private let header: [String: Int]
    private let row: Row
    private let columns: Columns
    private let rowNumber: Int
    private let options: ReadingOptions
    private let unkeyedDecoder: Decoder

    init(header: [String: Int], row: Row, columns: Columns, rowNumber: Int, options: ReadingOptions, unkeyedDecoder: Decoder) {
        self.header = header
        self.row = row
        self.columns = columns
        self.rowNumber = rowNumber
        self.options = options
        self.unkeyedDecoder = unkeyedDecoder
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
        guard let index = header[key.stringValue] else { return true }
        var isNil = row.isNil(at: index)
        if isNil, options.nilAsEmptyString, columns.type(at: index) == String.self {
            isNil = false
        }
        return isNil
    }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try decodePrim(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try decodeString(forKey: key) }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try decodePrim(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try decodePrim(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try decodePrim(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try decodePrim(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try decodePrim(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try decodePrim(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try decodePrim(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: Int128.Type,  forKey key: Key) throws -> Int128  { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try decodePrim(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try decodePrim(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: UInt128.Type, forKey key: Key) throws -> UInt128 { try decodePrim(type, forKey: key) }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        guard let value = try decodeValue(type, forKey: key, isRequired: true) else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { try decodeStringIfPresent(forKey: key) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { try decodePrimIfPresent(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decodeIfPresent(_ type: Int128.Type,  forKey key: Key) throws -> Int128?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { try decodePrimIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { try decodePrimIfPresent(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decodeIfPresent(_ type: UInt128.Type, forKey key: Key) throws -> UInt128? { try decodePrimIfPresent(type, forKey: key) }
    
    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        return try decodeValue(type, forKey: key, isRequired: false)
    }
    
    private func decodeValue<T: Decodable>(_ type: T.Type, forKey key: Key, isRequired: Bool) throws -> T? {
        if let parser = options.parserForType(type) {
            guard let string = try isRequired ? decodeString(forKey: key) : decodeStringIfPresent(forKey: key), !string.isEmpty || !isRequired else {
                return nil
            }
            guard let parsedValue = parser(string) else {
                throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
            }
            return parsedValue
        }

        guard let index = header[key.stringValue] else {
            return nil
        }

        if !isRequired && row.isNil(at: index) && !options.nilAsEmptyString {
            return nil
        }

        let decoder = DataSingleValueDecoder(index: index, key: key, row: row, rowNumber: rowNumber, options: options, introspector: nil)
        do {
            return try T(from: decoder)
        } catch CodableStringError.invalidFormat(let string) {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
    }
    
    private func decodePrim<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey) throws -> T {
        guard let index = header[key.stringValue] else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        guard let string = row[index, String.self, options] else {
            throw DataDecodingError.valueNotFound(type, forKey: key, rowNumber: rowNumber)
        }
        guard let value = T(string) else {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    private func decodePrimIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey) throws -> T? {
        guard let index = header[key.stringValue] else { return nil }
        return row[index, type, options]
    }
    
    private func decodeString(forKey key: CodingKey) throws -> String {
        guard let index = header[key.stringValue] else {
            throw DataDecodingError.valueNotFound(String.self, forKey: key, rowNumber: rowNumber)
        }
        var value = row[index, String.self, options]
        if value == nil, options.nilAsEmptyString {
            value = ""
        }
        guard let value else {
            throw DataDecodingError.valueNotFound(String.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    private func decodeStringIfPresent(forKey key: CodingKey) throws -> String? {
        guard let index = header[key.stringValue] else { return nil }
        return row[index, String.self, options] ?? (options.nilAsEmptyString ? "" : nil)
    }
    
    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) throws -> KeyedDecodingContainer<NestedKey>
    {
        throw DataDecodingError.nestedContainer(forKey: key, rowNumber: rowNumber)
    }
    
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DataDecodingError.nestedContainer(forKey: key, rowNumber: rowNumber)
    }
    
    func superDecoder() throws -> any Decoder {
        unkeyedDecoder
    }
    
    func superDecoder(forKey key: Key) throws -> any Decoder {
        throw DataDecodingError.superDecoder(forKey: key, rowNumber: rowNumber)
    }
}

fileprivate struct DataUnkeyedDecoder<Rows: DataRows, Columns: DataColumns>: Decoder, UnkeyedDecodingContainer {
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    var count: Int? { rows.count }
    var isAtEnd: Bool { currentIndex >= rows.count }
    var currentIndex: Int { currentRowIndex + 1 }

    private let header: [String: Int]?
    private let rows: Rows
    private let columns: Columns
    private let options: ReadingOptions
    private let introspector: DataDecodingIntrospector?
    private var rowIterator: Rows.Iterator
    private var currentRow: Rows.Element?
    private var currentRowIndex: Int = -1
    private var rowNumber: Int { currentRowIndex + (options.csvReadingOptions.hasHeaderRow ? 2 : 1) }

    init(header: [String: Int]?, rows: Rows, columns: Columns, options: ReadingOptions, introspector: DataDecodingIntrospector?) {
        self.header = header
        self.rows = rows
        self.columns = columns
        self.options = options
        self.introspector = introspector
        self.rowIterator = rows.makeIterator()
    }
    
    mutating func nextRow() {
        currentRowIndex += 1
        currentRow = rowIterator.next()
        introspector?.resetDecodeNilKey()
    }

    public func container<Key: CodingKey>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        guard let row = currentRow else {
            throw DataDecodingError.decoder("container", rowNumber: rowNumber)
        }

        let container: KeyedDecodingContainer<Key>

        if let introspector {
            container = KeyedDecodingContainer(
                IntrospectedDataKeyedDecoding(
                    header: header,
                    row: row,
                    columns: columns,
                    rowNumber: rowNumber,
                    options: options,
                    introspector: introspector,
                    unkeyedDecoder: self
                )
            )
        } else {
            container = KeyedDecodingContainer(
                DataKeyedDecoding(
                    header: header!,
                    row: row,
                    columns: columns,
                    rowNumber: rowNumber,
                    options: options,
                    unkeyedDecoder: self
                )
            )
        }

        return container
    }
    
    public func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        self
    }
    
    public func singleValueContainer() throws -> SingleValueDecodingContainer {
        guard let currentRow else {
            throw DataDecodingError.valueNotFound(String.self, rowNumber: rowNumber)
        }
        return DataSingleValueDecoder(index: 0, key: nil, row: currentRow, rowNumber: rowNumber, options: options, introspector: introspector)
    }
    
    mutating func decodeNil() throws -> Bool { false }
    
    mutating func decode(_ type: Bool.Type    ) throws -> Bool    { try decodePrim(type) }
    mutating func decode(_ type: String.Type  ) throws -> String  { try decodeString() }
    mutating func decode(_ type: Double.Type  ) throws -> Double  { try decodePrim(type) }
    mutating func decode(_ type: Float.Type   ) throws -> Float   { try decodePrim(type) }
    mutating func decode(_ type: Int.Type     ) throws -> Int     { try decodePrim(type) }
    mutating func decode(_ type: Int8.Type    ) throws -> Int8    { try decodePrim(type) }
    mutating func decode(_ type: Int16.Type   ) throws -> Int16   { try decodePrim(type) }
    mutating func decode(_ type: Int32.Type   ) throws -> Int32   { try decodePrim(type) }
    mutating func decode(_ type: Int64.Type   ) throws -> Int64   { try decodePrim(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decode(_ type: Int128.Type  ) throws -> Int128  { try decodePrim(type) }
    mutating func decode(_ type: UInt.Type    ) throws -> UInt    { try decodePrim(type) }
    mutating func decode(_ type: UInt8.Type   ) throws -> UInt8   { try decodePrim(type) }
    mutating func decode(_ type: UInt16.Type  ) throws -> UInt16  { try decodePrim(type) }
    mutating func decode(_ type: UInt32.Type  ) throws -> UInt32  { try decodePrim(type) }
    mutating func decode(_ type: UInt64.Type  ) throws -> UInt64  { try decodePrim(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decode(_ type: UInt128.Type ) throws -> UInt128 { try decodePrim(type) }

    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
        nextRow()
        return try T(from: self)
    }

    mutating func decodeIfPresent(_ type: Bool.Type    ) throws -> Bool?    { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type  ) throws -> String?  { try decodeStringIfPresent() }
    mutating func decodeIfPresent(_ type: Double.Type  ) throws -> Double?  { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: Float.Type   ) throws -> Float?   { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int.Type     ) throws -> Int?     { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int8.Type    ) throws -> Int8?    { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int16.Type   ) throws -> Int16?   { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int32.Type   ) throws -> Int32?   { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int64.Type   ) throws -> Int64?   { try decodePrimIfPresent(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decodeIfPresent(_ type: Int128.Type  ) throws -> Int128?  { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt.Type    ) throws -> UInt?    { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt8.Type   ) throws -> UInt8?   { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt16.Type  ) throws -> UInt16?  { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt32.Type  ) throws -> UInt32?  { try decodePrimIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt64.Type  ) throws -> UInt64?  { try decodePrimIfPresent(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decodeIfPresent(_ type: UInt128.Type ) throws -> UInt128? { try decodePrimIfPresent(type) }

    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? {
        nextRow()
        return try T(from: self)
    }
    
    private mutating func decodePrim<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        throw DataDecodingError.valueNotFound(T.self, rowNumber: rowNumber)
    }
    
    private mutating func decodePrimIfPresent<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        throw DataDecodingError.valueNotFound(T.self, rowNumber: rowNumber)
    }
    
    private mutating func decodeString() throws -> String {
        throw DataDecodingError.valueNotFound(String.self, rowNumber: rowNumber)
    }
    
    private mutating func decodeStringIfPresent() throws -> String? {
        throw DataDecodingError.valueNotFound(String.self, rowNumber: rowNumber)
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        throw DataDecodingError.nestedContainer(rowNumber: rowNumber)
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DataDecodingError.nestedContainer(rowNumber: rowNumber)
    }
    
    mutating func superDecoder() throws -> Decoder {
        self
    }
}

fileprivate struct DataSingleValueDecoder<Row: DataRow>: Decoder, SingleValueDecodingContainer {
    var codingPath: [CodingKey] = []
    var userInfo: [CodingUserInfoKey: Any] { [:] }

    private let index: Int
    private let key: CodingKey?
    private let row: Row
    private let rowNumber: Int
    private let options: ReadingOptions
    private let introspector: DataDecodingIntrospector?
    
    init(index: Int, key: CodingKey?, row: Row, rowNumber: Int, options: ReadingOptions, introspector: DataDecodingIntrospector?) {
        self.index = index
        self.key = key
        self.row = row
        self.rowNumber = rowNumber
        self.options = options
        self.introspector = introspector
    }

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        throw DataDecodingError.decoder("container", rowNumber: rowNumber)
    }

    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DataDecodingError.decoder("unkeyedContainer", rowNumber: rowNumber)
    }

    func singleValueContainer() throws -> SingleValueDecodingContainer {
        self
    }

    func decodeNil() -> Bool {
        row.isNil(at: index)
    }

    func decode(_ type: Bool.Type   ) throws -> Bool    { try decodePrim(type) }
    func decode(_ type: String.Type ) throws -> String  { try decodeString() }
    func decode(_ type: Double.Type ) throws -> Double  { try decodePrim(type) }
    func decode(_ type: Float.Type  ) throws -> Float   { try decodePrim(type) }
    func decode(_ type: Int.Type    ) throws -> Int     { try decodePrim(type) }
    func decode(_ type: Int8.Type   ) throws -> Int8    { try decodePrim(type) }
    func decode(_ type: Int16.Type  ) throws -> Int16   { try decodePrim(type) }
    func decode(_ type: Int32.Type  ) throws -> Int32   { try decodePrim(type) }
    func decode(_ type: Int64.Type  ) throws -> Int64   { try decodePrim(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: Int128.Type ) throws -> Int128  { try decodePrim(type) }
    func decode(_ type: UInt.Type   ) throws -> UInt    { try decodePrim(type) }
    func decode(_ type: UInt8.Type  ) throws -> UInt8   { try decodePrim(type) }
    func decode(_ type: UInt16.Type ) throws -> UInt16  { try decodePrim(type) }
    func decode(_ type: UInt32.Type ) throws -> UInt32  { try decodePrim(type) }
    func decode(_ type: UInt64.Type ) throws -> UInt64  { try decodePrim(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: UInt128.Type) throws -> UInt128 { try decodePrim(type) }

    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        if let parser = options.parserForType(type) {
            let string = try decodeString()
            guard let value = parser(string) else {
                throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
            }
            return value
        }

        do {
            return try T(from: self)
        } catch CodableStringError.invalidFormat(let string) {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
    }

    private func decodePrim<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        guard let value = row[index, type, options] else {
            throw DataDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        try introspector?.addCSVField(key: key, type: type, rowNumber: rowNumber)
        return value
    }
    
    private func decodeString() throws -> String {
        var value = row[index, String.self, options]
        if options.nilAsEmptyString, value == nil {
            value = ""
        }
        guard let value else {
            throw DataDecodingError.valueNotFound(String.self, forKey: key, rowNumber: rowNumber)
        }
        try introspector?.addCSVField(key: key, type: String.self, rowNumber: rowNumber)
        return value
    }
}
