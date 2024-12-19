//
//  TypeDecoder.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import TabularData

struct TypeDecoder {
    var types: DataTypes
    private let options: ReadingOptions

    init(options: ReadingOptions) {
        self.types = DataTypes()
        self.options = options
    }

    @discardableResult
    public func decode<T: Decodable>(
        _ type: T.Type,
        from strings: [String?],
        rowNumber: Int = -1) throws -> T
    {
        types.data = RowCollection(row: strings, rowNumber: rowNumber, rowMapping: nil, options: options)
        return try T(from: TypeDecoding(types: types))
    }
}

final class DataTypes {
    var data = RowCollection<[String?]>(row: [], rowNumber: -1, rowMapping: nil, options: .init())
    var csvTypes: [CSVType] = []
}

protocol CSVInitializable {
    init()
    
    static var csvType: CSVType { get }
}

extension Bool: CSVInitializable   { static var csvType: CSVType { .boolean } }
extension String: CSVInitializable { static var csvType: CSVType { .string  } }
extension Double: CSVInitializable { static var csvType: CSVType { .double  } }
extension Float: CSVInitializable  { static var csvType: CSVType { .float   } }
extension FixedWidthInteger where Self: CSVInitializable { static var csvType: CSVType { .integer } }

extension Int:    CSVInitializable {}
extension Int8:   CSVInitializable {}
extension Int16:  CSVInitializable {}
extension Int32:  CSVInitializable {}
extension Int64:  CSVInitializable {}
extension UInt:   CSVInitializable {}
extension UInt8:  CSVInitializable {}
extension UInt16: CSVInitializable {}
extension UInt32: CSVInitializable {}
extension UInt64: CSVInitializable {}

fileprivate struct TypeDecoding: DataDecoder {
    let types: DataTypes
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]
    
    init(types: DataTypes) { self.types = types }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        let container = TypeKeyedDecoding<Key>(decoding: self)
        return KeyedDecodingContainer(container)
    }
    func unkeyedContainer() -> UnkeyedDecodingContainer { TypeUnkeyedDecoding(decoding: self) }
    func singleValueContainer() -> SingleValueDecodingContainer { TypeSingleValueDecoding(decoding: self) }
    
    func nextString(forKey key: (any CodingKey)?) throws -> String { try types.data.nextString(forKey: key) }
    func nextStringIfPresent() -> String? { types.data.nextStringIfPresent() }
}

fileprivate struct TypeKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    private let decoding: TypeDecoding
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = [] 
    func contains(_ key: Key) -> Bool { true }

    init(decoding: TypeDecoding) { self.decoding = decoding }
    
    var types: DataTypes { decoding.types }

    private func addType<T: CSVInitializable>(_ type: T.Type, forKey key: Key) throws -> T {
        _ = try types.data.nextString(forKey: key)
        types.csvTypes.append(type.csvType)
        return T()
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
        _ = try types.data.nextString(forKey: key)
        return false
    }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try addType(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try addType(type, forKey: key) }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try addType(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try addType(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try addType(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try addType(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try addType(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try addType(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try addType(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try addType(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try addType(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try addType(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try addType(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try addType(type, forKey: key) }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        types.csvTypes.append(.string)
        return try types.data.decode(type, decoding: decoding)
    }

    private func addTypeIfPresent<T: CSVInitializable>(_ type: T.Type) -> T {
        _ = types.data.nextStringIfPresent()
        types.csvTypes.append(type.csvType)
        return T()
    }
    
    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { addTypeIfPresent(type) }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        _ = addTypeIfPresent(String.self)
        return nil
    }

    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedDecodingContainer<NestedKey>
    {
        KeyedDecodingContainer(TypeKeyedDecoding<NestedKey>(decoding: decoding))
    }
    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedDecodingContainer { TypeUnkeyedDecoding(decoding: decoding) }
    func superDecoder() throws -> any Decoder { try superDecoder(forKey: Key(stringValue: "super")!) }
    func superDecoder(forKey key: Key) throws -> any Decoder { decoding }
}

fileprivate struct TypeUnkeyedDecoding: UnkeyedDecodingContainer {
    private let decoding: TypeDecoding
    var codingPath: [CodingKey] = []
    var count: Int? { return types.data.row.count }
    var isAtEnd: Bool { return currentIndex >= types.data.row.count }
    var currentIndex: Int { return types.data.currentIndex }

    init(decoding: TypeDecoding) { self.decoding = decoding }

    var types: DataTypes { decoding.types }

    private func checkEnd() throws {
        if isAtEnd {
            throw CSVDecodingError.dataCorrupted(
                DecodingError.Context(codingPath: [],
                                      debugDescription: "Unkeyed container is at end\(types.data.rowNumber.atRow)."))
        }
    }

    private func addType<T: CSVInitializable>(_ type: T.Type) throws -> T {
        _ = try types.data.nextString()
        types.csvTypes.append(type.csvType)
        return T()
    }
    
    mutating func decodeNil() throws -> Bool {
        _ = try types.data.nextString()
        return false
    }
    
    mutating func decode(_ type: Bool.Type   ) throws -> Bool    { try addType(type) }
    mutating func decode(_ type: String.Type ) throws -> String  { try addType(type) }
    mutating func decode(_ type: Double.Type ) throws -> Double  { try addType(type) }
    mutating func decode(_ type: Float.Type  ) throws -> Float   { try addType(type) }
    mutating func decode(_ type: Int.Type    ) throws -> Int     { try addType(type) }
    mutating func decode(_ type: Int8.Type   ) throws -> Int8    { try addType(type) }
    mutating func decode(_ type: Int16.Type  ) throws -> Int16   { try addType(type) }
    mutating func decode(_ type: Int32.Type  ) throws -> Int32   { try addType(type) }
    mutating func decode(_ type: Int64.Type  ) throws -> Int64   { try addType(type) }
    mutating func decode(_ type: UInt.Type   ) throws -> UInt    { try addType(type) }
    mutating func decode(_ type: UInt8.Type  ) throws -> UInt8   { try addType(type) }
    mutating func decode(_ type: UInt16.Type ) throws -> UInt16  { try addType(type) }
    mutating func decode(_ type: UInt32.Type ) throws -> UInt32  { try addType(type) }
    mutating func decode(_ type: UInt64.Type ) throws -> UInt64  { try addType(type) }

    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        types.csvTypes.append(.string)
        return try types.data.decode(type, decoding: decoding)
    }

    private func addTypeIfPresent<T: CSVInitializable>(_ type: T.Type) -> T {
        _ = types.data.nextStringIfPresent()
        types.csvTypes.append(type.csvType)
        return T()
    }
    
    mutating func decodeIfPresent(_ type: Bool.Type   ) throws -> Bool?    { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type ) throws -> String?  { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Double.Type ) throws -> Double?  { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Float.Type  ) throws -> Float?   { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int.Type    ) throws -> Int?     { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int8.Type   ) throws -> Int8?    { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int16.Type  ) throws -> Int16?   { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int32.Type  ) throws -> Int32?   { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int64.Type  ) throws -> Int64?   { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt.Type   ) throws -> UInt?    { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt8.Type  ) throws -> UInt8?   { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt16.Type ) throws -> UInt16?  { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt32.Type ) throws -> UInt32?  { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt64.Type ) throws -> UInt64?  { addTypeIfPresent(type) }

    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? {
        _ = addTypeIfPresent(String.self)
        return nil
    }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        KeyedDecodingContainer(TypeKeyedDecoding<NestedKey>(decoding: decoding))
    }
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer { TypeUnkeyedDecoding(decoding: decoding) }
    mutating func superDecoder() throws -> Decoder { decoding }
}

fileprivate struct TypeSingleValueDecoding: SingleValueDecodingContainer {
    private let decoding: TypeDecoding
    var codingPath: [CodingKey] = []

    init(decoding: TypeDecoding) { self.decoding = decoding }

    var types: DataTypes { decoding.types }

    private func addType<T: CSVInitializable>(_ type: T.Type) throws -> T {
        _ = try types.data.nextString()
        types.csvTypes.append(type.csvType)
        return T()
    }
    
    func decodeNil() -> Bool {
        _ = types.data.nextStringIfPresent()
        return false
    }
    
    func decode(_ type: Bool.Type   ) throws -> Bool    { try addType(type) }
    func decode(_ type: String.Type ) throws -> String  { try addType(type) }
    func decode(_ type: Double.Type ) throws -> Double  { try addType(type) }
    func decode(_ type: Float.Type  ) throws -> Float   { try addType(type) }
    func decode(_ type: Int.Type    ) throws -> Int     { try addType(type) }
    func decode(_ type: Int8.Type   ) throws -> Int8    { try addType(type) }
    func decode(_ type: Int16.Type  ) throws -> Int16   { try addType(type) }
    func decode(_ type: Int32.Type  ) throws -> Int32   { try addType(type) }
    func decode(_ type: Int64.Type  ) throws -> Int64   { try addType(type) }
    func decode(_ type: UInt.Type   ) throws -> UInt    { try addType(type) }
    func decode(_ type: UInt8.Type  ) throws -> UInt8   { try addType(type) }
    func decode(_ type: UInt16.Type ) throws -> UInt16  { try addType(type) }
    func decode(_ type: UInt32.Type ) throws -> UInt32  { try addType(type) }
    func decode(_ type: UInt64.Type ) throws -> UInt64  { try addType(type) }
    
    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        types.csvTypes.append(.string)
        return try types.data.decode(type, decoding: decoding)
    }
}
