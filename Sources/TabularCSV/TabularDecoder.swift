//
//  TabularDecoder.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Based partially on: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

import TabularData

public typealias DataFrameDecoder = TabularDecoder<DataFrame.Rows>

public typealias StringDecoder = TabularDecoder<[[String?]]>

protocol CSVPrimitiveType {
    static var csvType: CSVType { get }
}

extension Bool:   CSVPrimitiveType { static var csvType: CSVType { .boolean } }
extension String: CSVPrimitiveType { static var csvType: CSVType { .string  } }
extension Double: CSVPrimitiveType { static var csvType: CSVType { .double  } }
extension Float:  CSVPrimitiveType { static var csvType: CSVType { .float   } }
extension FixedWidthInteger where Self: CSVPrimitiveType { static var csvType: CSVType { .integer } }

extension Int:    CSVPrimitiveType {}
extension Int8:   CSVPrimitiveType {}
extension Int16:  CSVPrimitiveType {}
extension Int32:  CSVPrimitiveType {}
extension Int64:  CSVPrimitiveType {}
extension UInt:   CSVPrimitiveType {}
extension UInt8:  CSVPrimitiveType {}
extension UInt16: CSVPrimitiveType {}
extension UInt32: CSVPrimitiveType {}
extension UInt64: CSVPrimitiveType {}


public struct TabularDecoder<Rows: TypedRows> {
    private let options: ReadingOptions
    
    public init(options: ReadingOptions) { self.options = options }
    
    public func decode<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        rowMapping: [Int?]? = nil) throws -> T
    {
        try T(from: TabularRowsDecoder(rows: rows, rowMapping: rowMapping, withTypes: false, options: options))
    }

    func decodeWithTypes<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        rowMapping: [Int?]? = nil) throws -> (value: T, csvTypes: [CSVType])
    {
        let decoder = TabularRowsDecoder(rows: rows, rowMapping: rowMapping, withTypes: true, options: options)
        let value = try T(from: decoder)
        return (value: value, csvTypes: decoder.data.csvTypes ?? [])
    }
}

struct TabularRowsDecoder<Rows: TypedRows>: Decoder {
    fileprivate let data: RowCollection<Rows>
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    init(rows: Rows, rowMapping: [Int?]?, withTypes: Bool, options: ReadingOptions) {
        self.data = RowCollection(rows: rows, rowMapping: rowMapping, withTypes: withTypes, options: options)
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(TabularKeyedDecoding<Key, Rows>(decoder: self))
    }
    
    func unkeyedContainer() -> UnkeyedDecodingContainer {
        TabularUnkeyedDecoding(decoder: self)
    }
    
    func singleValueContainer() -> SingleValueDecodingContainer {
        TabularSingleValueDecoding(decoder: self)
    }
}

fileprivate struct TabularKeyedDecoding<Key: CodingKey, Rows: TypedRows>: KeyedDecodingContainerProtocol {
    private let decoder: TabularRowsDecoder<Rows>
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { true }
    
    init(decoder: TabularRowsDecoder<Rows>) { self.decoder = decoder }
    
    var data: RowCollection<Rows> { decoder.data }

    func decodeNil(forKey key: Key) throws -> Bool { try data.nextValue(String.self, forKey: key) == "nil" }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try data.nextValue(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try data.nextValue(type, forKey: key) }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try data.nextValue(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try data.nextValue(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try data.nextValue(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try data.nextValue(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try data.nextValue(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try data.nextValue(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try data.nextValue(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try data.nextValue(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try data.nextValue(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try data.nextValue(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try data.nextValue(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try data.nextValue(type, forKey: key) }

    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        try data.decode(type, forKey: key, decoder: decoder)
    }

    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { data.nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { data.nextValueIfPresent(type) }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        try data.decodeIfPresent(type, decoder: decoder)
    }
    
    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) throws -> KeyedDecodingContainer<NestedKey>
    {
        throw CSVDecodingError.nestedContainer(forKey: key, rowNumber: data.rowNumber)
    }
    
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw CSVDecodingError.nestedContainer(forKey: key, rowNumber: data.rowNumber)
    }

    func superDecoder() throws -> any Decoder {
        try superDecoder(forKey: Key(stringValue: "super")!)
    }

    func superDecoder(forKey key: Key) throws -> any Decoder {
        decoder
    }
}

fileprivate struct TabularUnkeyedDecoding<Rows: TypedRows>: UnkeyedDecodingContainer {
    private let decoder: TabularRowsDecoder<Rows>
    var codingPath: [CodingKey] = []
    var count: Int? { return data.rowCount }
    var isAtEnd: Bool { return data.currentRowIndex >= data.rowCount }
    var currentIndex: Int { return data.currentRowIndex }
    
    init(decoder: TabularRowsDecoder<Rows>) { self.decoder = decoder }
    
    var data: RowCollection<Rows> { decoder.data }
    
    private func checkEnd() throws {
        if isAtEnd {
            throw CSVDecodingError.isAtEnd(rowNumber: data.rowNumber)
        }
    }
    
    private func nextValue<T: LosslessStringConvertible & CSVPrimitiveType>(_ type: T.Type) throws -> T {
        try data.nextRow()
        return try data.nextValue(type)
    }
    
    mutating func decodeNil() throws -> Bool {
        try data.nextRow()
        return try nextValue(String.self) == "nil"
    }
    
    mutating func decode(_ type: Bool.Type    ) throws -> Bool    { try nextValue(type) }
    mutating func decode(_ type: String.Type  ) throws -> String  { try nextValue(type) }
    mutating func decode(_ type: Double.Type  ) throws -> Double  { try nextValue(type) }
    mutating func decode(_ type: Float.Type   ) throws -> Float   { try nextValue(type) }
    mutating func decode(_ type: Int.Type     ) throws -> Int     { try nextValue(type) }
    mutating func decode(_ type: Int8.Type    ) throws -> Int8    { try nextValue(type) }
    mutating func decode(_ type: Int16.Type   ) throws -> Int16   { try nextValue(type) }
    mutating func decode(_ type: Int32.Type   ) throws -> Int32   { try nextValue(type) }
    mutating func decode(_ type: Int64.Type   ) throws -> Int64   { try nextValue(type) }
    mutating func decode(_ type: UInt.Type    ) throws -> UInt    { try nextValue(type) }
    mutating func decode(_ type: UInt8.Type   ) throws -> UInt8   { try nextValue(type) }
    mutating func decode(_ type: UInt16.Type  ) throws -> UInt16  { try nextValue(type) }
    mutating func decode(_ type: UInt32.Type  ) throws -> UInt32  { try nextValue(type) }
    mutating func decode(_ type: UInt64.Type  ) throws -> UInt64  { try nextValue(type) }

    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try data.nextRow()
        return try data.decode(type, decoder: decoder)
    }

    private func nextValueIfPresent<T: LosslessStringConvertible & CSVPrimitiveType>(_ type: T.Type) -> T? {
        guard data.nextRowIfPresent() else { return nil }
        return data.nextValueIfPresent(type)
    }

    mutating func decodeIfPresent(_ type: Bool.Type    ) throws -> Bool?    { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type  ) throws -> String?  { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Double.Type  ) throws -> Double?  { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Float.Type   ) throws -> Float?   { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int.Type     ) throws -> Int?     { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int8.Type    ) throws -> Int8?    { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int16.Type   ) throws -> Int16?   { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int32.Type   ) throws -> Int32?   { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int64.Type   ) throws -> Int64?   { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt.Type    ) throws -> UInt?    { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt8.Type   ) throws -> UInt8?   { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt16.Type  ) throws -> UInt16?  { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt32.Type  ) throws -> UInt32?  { nextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt64.Type  ) throws -> UInt64?  { nextValueIfPresent(type) }

    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? {
        try data.nextRow()
        return try data.decodeIfPresent(type, decoder: decoder)
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        try checkEnd()
        return KeyedDecodingContainer(TabularKeyedDecoding<NestedKey, Rows>(decoder: decoder))
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw CSVDecodingError.nestedContainer(rowNumber: data.rowNumber)
    }
    
    mutating func superDecoder() throws -> Decoder {
        decoder
    }
}

fileprivate struct TabularSingleValueDecoding<Rows: TypedRows>: SingleValueDecodingContainer {
    private let decoder: TabularRowsDecoder<Rows>
    private let index: RowCollection<Rows>.ValueIndex
    var codingPath: [CodingKey] = []

    init(decoder: TabularRowsDecoder<Rows>) {
        self.decoder = decoder
        self.index = decoder.data.getValueIndex()
    }
    
    var data: RowCollection<Rows> { decoder.data }
    
    private func nextValue<T: LosslessStringConvertible & CSVPrimitiveType>(_ type: T.Type) throws -> T {
        try data.checkValueIndex(index)
        return try data.nextValue(type)
    }

    func decodeNil() -> Bool {
        guard data.checkValueIndexIfPresent(index) else { return false }
        return data.nextValueIfPresent(String.self) == "nil"
    }
    
    func decode(_ type: Bool.Type   ) throws -> Bool    { try nextValue(type) }
    func decode(_ type: String.Type ) throws -> String  { try nextValue(type) }
    func decode(_ type: Double.Type ) throws -> Double  { try nextValue(type) }
    func decode(_ type: Float.Type  ) throws -> Float   { try nextValue(type) }
    func decode(_ type: Int.Type    ) throws -> Int     { try nextValue(type) }
    func decode(_ type: Int8.Type   ) throws -> Int8    { try nextValue(type) }
    func decode(_ type: Int16.Type  ) throws -> Int16   { try nextValue(type) }
    func decode(_ type: Int32.Type  ) throws -> Int32   { try nextValue(type) }
    func decode(_ type: Int64.Type  ) throws -> Int64   { try nextValue(type) }
    func decode(_ type: UInt.Type   ) throws -> UInt    { try nextValue(type) }
    func decode(_ type: UInt8.Type  ) throws -> UInt8   { try nextValue(type) }
    func decode(_ type: UInt16.Type ) throws -> UInt16  { try nextValue(type) }
    func decode(_ type: UInt32.Type ) throws -> UInt32  { try nextValue(type) }
    func decode(_ type: UInt64.Type ) throws -> UInt64  { try nextValue(type) }

    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try data.checkValueIndex(index)
        return try data.decode(type, decoder: decoder)
    }
}
