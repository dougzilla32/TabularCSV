//
//  StringDecoder.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Based on: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

import Foundation
import TabularData

typealias TabularRow = DataCollection<DataFrame.Row>

/// An object that decodes strings following the simple strings file format
/// into instances of a data type.
public class DataFrameDecoder {
    private let options: ReadingOptions

    public init(options: ReadingOptions) {
        self.options = options
    }

    /// Decodes a strings file-encoded representation into an instance of the specified type.
    public func decode<T: Decodable>(
        _ type: T.Type,
        from row: DataFrame.Row,
        rowNumber: Int = -1,
        rowMapping: [Int?]? = nil) throws -> T
    {
        try T(from: DataFrameDecoding(row: row, rowNumber: rowNumber, rowMapping: rowMapping, options: options))
    }
    
}

fileprivate struct DataFrameDecoding: Decoder {

    private let data: TabularRow
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    init(row: DataFrame.Row, rowNumber: Int, rowMapping: [Int?]?, options: ReadingOptions) {
        self.data = DataCollection(row: row, rowNumber: rowNumber, rowMapping: rowMapping, options: options)
    }
    
    init(data: TabularRow) {
        self.data = data
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        let container = DataFrameKeyedDecoding<Key>(from: data)
        return KeyedDecodingContainer(container)
    }

    func unkeyedContainer() -> UnkeyedDecodingContainer {
        return DataFrameUnkeyedDecoding(from: data)
    }

    func singleValueContainer() -> SingleValueDecodingContainer {
        return DataFrameSingleValueDecoding(from: data)
    }
}

fileprivate struct DataFrameKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    private let data: TabularRow
    var codingPath: [CodingKey] = []
    
    init(from data: TabularRow) { self.data = data }

    var allKeys: [Key] { [] }

    func contains(_ key: Key) -> Bool { true }
    
    private func nextValue<T: LosslessStringConvertible>(_ type: T.Type, forKey key: Key) throws -> T {
        try data.nextValue(type, forKey: key)
    }
    
    func decodeNil(forKey key: Key) throws -> Bool { try nextValue(String.self, forKey: key) == "nil" }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try nextValue(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try nextValue(type, forKey: key) }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try nextValue(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try nextValue(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try nextValue(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try nextValue(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try nextValue(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try nextValue(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try nextValue(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try nextValue(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try nextValue(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try nextValue(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try nextValue(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try nextValue(type, forKey: key) }
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T { try data.decode(type) }

    private func nextValueIfPresent<T: LosslessStringConvertible>(_ type: T.Type) -> T? {
        data.nextValueIfPresent(type)
    }

    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { nextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { nextValueIfPresent(type) }
    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? { try data.decodeIfPresent(type) }
    
    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedDecodingContainer<NestedKey>
    {
        KeyedDecodingContainer(DataFrameKeyedDecoding<NestedKey>(from: data))
    }
    
    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedDecodingContainer {
        DataFrameUnkeyedDecoding(from: data)
    }

    func superDecoder() throws -> any Decoder {
        return try superDecoder(forKey: Key(stringValue: "super")!)
    }
    
    func superDecoder(forKey key: Key) throws -> any Decoder {
        DataFrameDecoding(data: data)
    }
}

fileprivate struct DataFrameUnkeyedDecoding: UnkeyedDecodingContainer {
    private let data: TabularRow
    var codingPath: [CodingKey] = []
    var count: Int? { return data.row.count }
    var isAtEnd: Bool { return currentIndex >= data.row.count }
    var currentIndex: Int { return data.currentIndex }
    
    init(from data: TabularRow) {
        self.data = data
    }
    
    private func checkEnd() throws {
        if isAtEnd {
            throw CSVDecodingError.dataCorrupted(
                DecodingError.Context(codingPath: [],
                                      debugDescription: "Unkeyed container is at end\(data.rowNumber.atRow)."))
        }
    }
    
    private func nextValue<T: LosslessStringConvertible>(_ type: T.Type) throws -> T {
        try data.nextValue(type)
    }
    
    mutating func decodeNil() throws -> Bool { try nextValue(String.self) == "nil" }
    
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
    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T { try data.decode(type) }

    private func nextValueIfPresent<T: LosslessStringConvertible>(_ type: T.Type) -> T? {
        data.nextValueIfPresent(type)
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
    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? { try data.decodeIfPresent(type) }
    
    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        try checkEnd()
        return KeyedDecodingContainer(DataFrameKeyedDecoding<NestedKey>(from: data))
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        try checkEnd()
        return DataFrameUnkeyedDecoding(from: data)
    }
    
    mutating func superDecoder() throws -> Decoder {
        try checkEnd()
        return DataFrameDecoding(data: data)
    }
}

fileprivate struct DataFrameSingleValueDecoding: SingleValueDecodingContainer {
    
    private let data: TabularRow
    var codingPath: [CodingKey] = []

    init(from data: TabularRow) {
        self.data = data
    }
    
    private func nextValue<T: LosslessStringConvertible>(_ type: T.Type) throws -> T {
        try data.nextValue(type)
    }

    func decodeNil() -> Bool { data.nextValueIfPresent(String.self) == "nil" }
    
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
    func decode<T: Decodable>(_ type: T.Type) throws -> T { try data.decode(type) }
}
