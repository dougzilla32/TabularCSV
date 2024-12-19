//
//  DataFrameDecoder.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Based on: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

import Foundation
import TabularData

public struct DataFrameDecoder {
    private let options: ReadingOptions

    public init(options: ReadingOptions) { self.options = options }

    public func decode<T: Decodable>(
        _ type: T.Type,
        dataFrame: DataFrame,
        rowMapping: [Int?]? = nil) throws -> [T]
    {
        try dataFrame.rows.enumerated().map { index, row in
            try decode(T.self, row: row, rowNumber: index+1, rowMapping: rowMapping)
        }
    }
    
    public func decode<T: Decodable>(
        _ type: T.Type,
        row: DataFrame.Row,
        rowNumber: Int = -1,
        rowMapping: [Int?]? = nil) throws -> T
    {
        try T(from: DataFrameDecoding(row: row, rowNumber: rowNumber, rowMapping: rowMapping, options: options))
    }
}

struct DataFrameDecoding: DataDecoder {
    let data: RowCollection<DataFrame.Row>
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    init(row: DataFrame.Row, rowNumber: Int, rowMapping: [Int?]?, options: ReadingOptions) {
        self.data = RowCollection(row: row, rowNumber: rowNumber, rowMapping: rowMapping, options: options)
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(DataFrameKeyedDecoding<Key>(decoding: self))
    }
    func unkeyedContainer() -> UnkeyedDecodingContainer { DataFrameUnkeyedDecoding(decoding: self) }
    func singleValueContainer() -> SingleValueDecodingContainer { DataFrameSingleValueDecoding(decoding: self) }

    func nextString(forKey key: CodingKey?) throws -> String { try data.nextString(forKey: key) }
    func nextStringIfPresent() -> String? { data.nextStringIfPresent() }
}

fileprivate struct DataFrameKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    private let decoding: DataFrameDecoding
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { true }

    init(decoding: DataFrameDecoding) { self.decoding = decoding }
    
    private func nextValue<T: LosslessStringConvertible>(_ type: T.Type, forKey key: Key) throws -> T {
        try decoding.data.nextValue(type, forKey: key)
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
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T { try decoding.data.decode(type, forKey: key, decoding: decoding) }

    private func nextValueIfPresent<T: LosslessStringConvertible>(_ type: T.Type) -> T? {
        decoding.data.nextValueIfPresent(type)
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
    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? { try decoding.data.decodeIfPresent(type, decoding: decoding) }
    
    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedDecodingContainer<NestedKey>
    {
        KeyedDecodingContainer(DataFrameKeyedDecoding<NestedKey>(decoding: decoding))
    }    
    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedDecodingContainer { DataFrameUnkeyedDecoding(decoding: decoding) }
    func superDecoder() throws -> any Decoder { try superDecoder(forKey: Key(stringValue: "super")!) }
    func superDecoder(forKey key: Key) throws -> any Decoder { decoding }
}

fileprivate struct DataFrameUnkeyedDecoding: UnkeyedDecodingContainer {
    private let decoding: DataFrameDecoding
    var codingPath: [CodingKey] = []
    var count: Int? { return decoding.data.row.count }
    var isAtEnd: Bool { return currentIndex >= decoding.data.row.count }
    var currentIndex: Int { return decoding.data.currentIndex }
    
    init(decoding: DataFrameDecoding) { self.decoding = decoding }
    
    private func checkEnd() throws {
        if isAtEnd {
            throw CSVDecodingError.dataCorrupted(
                DecodingError.Context(codingPath: [],
                                      debugDescription: "Unkeyed container is at end\(decoding.data.rowNumber.atRow)."))
        }
    }
    
    private func nextValue<T: LosslessStringConvertible>(_ type: T.Type) throws -> T {
        try decoding.data.nextValue(type)
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
    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T { try decoding.data.decode(type, decoding: decoding) }

    private func nextValueIfPresent<T: LosslessStringConvertible>(_ type: T.Type) -> T? {
        decoding.data.nextValueIfPresent(type)
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
    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? { try decoding.data.decodeIfPresent(type, decoding: decoding) }
    
    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        try checkEnd()
        return KeyedDecodingContainer(DataFrameKeyedDecoding<NestedKey>(decoding: decoding))
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        try checkEnd()
        return DataFrameUnkeyedDecoding(decoding: decoding)
    }
    
    mutating func superDecoder() throws -> Decoder {
        try checkEnd()
        return decoding
    }
}

fileprivate struct DataFrameSingleValueDecoding: SingleValueDecodingContainer {
    private let decoding: DataFrameDecoding
    var codingPath: [CodingKey] = []

    init(decoding: DataFrameDecoding) { self.decoding = decoding }
    
    private func nextValue<T: LosslessStringConvertible>(_ type: T.Type) throws -> T {
        try decoding.data.nextValue(type)
    }

    func decodeNil() -> Bool { decoding.data.nextValueIfPresent(String.self) == "nil" }
    
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
    func decode<T: Decodable>(_ type: T.Type) throws -> T { try decoding.data.decode(type, decoding: decoding) }
}
