//
//  StringDecoder.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Based on: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

import Foundation

typealias StringRow = DataCollection<[String?]>

public class StringDecoder {
    private let options: ReadingOptions

    public init(options: ReadingOptions) {
        self.options = options
    }

    public func decode<T: Decodable>(
        _ type: T.Type,
        from strings: [String?],
        rowNumber: Int = -1) throws -> T
    {
        try T(from: StringDecoding(strings: strings, rowNumber: rowNumber, options: options))
    }
}

struct StringDecoding: Decoder {
    private let data: StringRow
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    init(strings: [String?], rowNumber: Int, options: ReadingOptions) {
        self.data = DataCollection(row: strings, rowNumber: rowNumber, rowMapping: nil, options: options)
    }
    
    init(data: StringRow) {
        self.data = data
    }

    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        let container = StringKeyedDecoding<Key>(from: data)
        return KeyedDecodingContainer(container)
    }

    func unkeyedContainer() -> UnkeyedDecodingContainer {
        return StringUnkeyedDecoding(from: data)
    }

    func singleValueContainer() -> SingleValueDecodingContainer {
        return StringSingleValueDecoding(from: data)
    }
}

fileprivate struct StringKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    private let data: StringRow
    var codingPath: [CodingKey] = []
    var allKeys: [Key] { [] }

    init(from data: StringRow) {
        self.data = data
    }
    
    func contains(_ key: Key) -> Bool { true  }

    private func decodeNextValue<T: LosslessStringConvertible>(_ type: T.Type, forKey key: Key) throws -> T {
        try data.decode(type, forKey: key)
    }

    func decodeNil(forKey key: Key) throws -> Bool { try decodeNextValue(String.self, forKey: key) == "nil" }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try decodeNextValue(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try data.nextString() }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try decodeNextValue(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try decodeNextValue(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try decodeNextValue(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try decodeNextValue(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try decodeNextValue(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try decodeNextValue(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try decodeNextValue(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try decodeNextValue(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try decodeNextValue(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try decodeNextValue(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try decodeNextValue(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try decodeNextValue(type, forKey: key) }
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T { try data.decode(type, forKey: key) }
    
    private func decodeNextValueIfPresent<T: LosslessStringConvertible>(_ type: T.Type) throws -> T? {
        try data.decodeIfPresent(type)
    }

    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { data.nextStringIfPresent() }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { try decodeNextValueIfPresent(type) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { try decodeNextValueIfPresent(type) }
    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? { try data.decodeIfPresent(type) }

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedDecodingContainer<NestedKey>
    {
        KeyedDecodingContainer(StringKeyedDecoding<NestedKey>(from: data))
    }
    
    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedDecodingContainer {
        StringUnkeyedDecoding(from: data)
    }

    func superDecoder() throws -> any Decoder {
        try superDecoder(forKey: Key(stringValue: "super")!)
    }
    
    func superDecoder(forKey key: Key) throws -> any Decoder {
        StringDecoding(data: data)
    }
}

fileprivate struct StringUnkeyedDecoding: UnkeyedDecodingContainer {
    private let data: StringRow
    var codingPath: [CodingKey] = []
    var count: Int? { data.row.count }
    var isAtEnd: Bool { currentIndex >= data.row.count }
    var currentIndex: Int { data.currentIndex }
    
    init(from data: StringRow) {
        self.data = data
    }
    
    private func checkEnd() throws {
        if isAtEnd {
            throw CSVDecodingError.dataCorrupted(
                DecodingError.Context(codingPath: [],
                                      debugDescription: "Unkeyed container is at end\(data.rowNumber.atRow)."))
        }
    }
    
    private func decodeNextValue<T: LosslessStringConvertible>(_ type: T.Type) throws -> T {
        try data.decode(type)
    }

    mutating func decodeNil() throws -> Bool { try decodeNextValue(String.self) == "nil" }
    
    mutating func decode(_ type: Bool.Type    ) throws -> Bool    { try decodeNextValue(type) }
    mutating func decode(_ type: String.Type  ) throws -> String  { try data.nextString() }
    mutating func decode(_ type: Double.Type  ) throws -> Double  { try decodeNextValue(type) }
    mutating func decode(_ type: Float.Type   ) throws -> Float   { try decodeNextValue(type) }
    mutating func decode(_ type: Int.Type     ) throws -> Int     { try decodeNextValue(type) }
    mutating func decode(_ type: Int8.Type    ) throws -> Int8    { try decodeNextValue(type) }
    mutating func decode(_ type: Int16.Type   ) throws -> Int16   { try decodeNextValue(type) }
    mutating func decode(_ type: Int32.Type   ) throws -> Int32   { try decodeNextValue(type) }
    mutating func decode(_ type: Int64.Type   ) throws -> Int64   { try decodeNextValue(type) }
    mutating func decode(_ type: UInt.Type    ) throws -> UInt    { try decodeNextValue(type) }
    mutating func decode(_ type: UInt8.Type   ) throws -> UInt8   { try decodeNextValue(type) }
    mutating func decode(_ type: UInt16.Type  ) throws -> UInt16  { try decodeNextValue(type) }
    mutating func decode(_ type: UInt32.Type  ) throws -> UInt32  { try decodeNextValue(type) }
    mutating func decode(_ type: UInt64.Type  ) throws -> UInt64  { try decodeNextValue(type) }
    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T { try data.decode(type) }
    
    private func decodeNextValueIfPresent<T: LosslessStringConvertible>(_ type: T.Type) throws -> T? {
        try data.decodeIfPresent(type)
    }

    mutating func decodeIfPresent(_ type: Bool.Type    ) throws -> Bool?    { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type  ) throws -> String?  { data.nextStringIfPresent()  }
    mutating func decodeIfPresent(_ type: Double.Type  ) throws -> Double?  { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Float.Type   ) throws -> Float?   { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int.Type     ) throws -> Int?     { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int8.Type    ) throws -> Int8?    { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int16.Type   ) throws -> Int16?   { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int32.Type   ) throws -> Int32?   { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int64.Type   ) throws -> Int64?   { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt.Type    ) throws -> UInt?    { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt8.Type   ) throws -> UInt8?   { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt16.Type  ) throws -> UInt16?  { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt32.Type  ) throws -> UInt32?  { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt64.Type  ) throws -> UInt64?  { try decodeNextValueIfPresent(type) }
    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? { try data.decodeIfPresent(type) }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        try checkEnd()
        return KeyedDecodingContainer(StringKeyedDecoding<NestedKey>(from: data))
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        try checkEnd()
        return StringUnkeyedDecoding(from: data)
    }
    
    mutating func superDecoder() throws -> Decoder {
        try checkEnd()
        return StringDecoding(data: data)
    }
}

fileprivate struct StringSingleValueDecoding: SingleValueDecodingContainer {
    private let data: StringRow
    var codingPath: [CodingKey] = []

    init(from data: StringRow) {
        self.data = data
    }

    private func decodeNextValue<T: LosslessStringConvertible>(_ type: T.Type) throws -> T {
        try data.decode(type)
    }

    func decodeNil() -> Bool { data.nextValueIfPresent(String.self) == "nil" }
    
    func decode(_ type: Bool.Type   ) throws -> Bool    { try decodeNextValue(type) }
    func decode(_ type: String.Type ) throws -> String  { try data.nextString() }
    func decode(_ type: Double.Type ) throws -> Double  { try decodeNextValue(type) }
    func decode(_ type: Float.Type  ) throws -> Float   { try decodeNextValue(type) }
    func decode(_ type: Int.Type    ) throws -> Int     { try decodeNextValue(type) }
    func decode(_ type: Int8.Type   ) throws -> Int8    { try decodeNextValue(type) }
    func decode(_ type: Int16.Type  ) throws -> Int16   { try decodeNextValue(type) }
    func decode(_ type: Int32.Type  ) throws -> Int32   { try decodeNextValue(type) }
    func decode(_ type: Int64.Type  ) throws -> Int64   { try decodeNextValue(type) }
    func decode(_ type: UInt.Type   ) throws -> UInt    { try decodeNextValue(type) }
    func decode(_ type: UInt8.Type  ) throws -> UInt8   { try decodeNextValue(type) }
    func decode(_ type: UInt16.Type ) throws -> UInt16  { try decodeNextValue(type) }
    func decode(_ type: UInt32.Type ) throws -> UInt32  { try decodeNextValue(type) }
    func decode(_ type: UInt64.Type ) throws -> UInt64  { try decodeNextValue(type) }
    func decode<T: Decodable>(_ type: T.Type) throws -> T { try data.decode(type) }
}
