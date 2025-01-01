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
    public func decode<T: Decodable & Collection>(
        _ type: T.Type,
        from strings: [String?]) throws -> T
    {
        types.data = RowCollection<[[String?]]>(rows: [strings], rowMapping: nil, options: options)
        types.csvTypes = []
        return try T(from: TypeRowsDecoder(types: types))
    }
}

final class DataTypes {
    var data = RowCollection<[[String?]]>(rows: [], rowMapping: nil, options: .init())
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

fileprivate struct TypeRowsDecoder: Decoder {
    let types: DataTypes
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]
    
    init(types: DataTypes) { self.types = types }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(TypeKeyedDecoding<Key>(decoder: self))
    }
    func unkeyedContainer() -> UnkeyedDecodingContainer { TypeUnkeyedDecoding(decoder: self) }
    func singleValueContainer() -> SingleValueDecodingContainer { TypeSingleValueDecoding(decoder: self) }
}

fileprivate struct TypeKeyedDecoding<Key: CodingKey>: KeyedDecodingContainerProtocol {
    private let decoder: TypeRowsDecoder
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = [] 
    func contains(_ key: Key) -> Bool { true }

    init(decoder: TypeRowsDecoder) { self.decoder = decoder }
    
    var types: DataTypes { decoder.types }
    
    var data: RowCollection<[[String?]]> { types.data }

    private func addType<T: CSVInitializable>(_ type: T.Type, forKey key: Key) throws -> T {
        types.csvTypes.append(type.csvType)
        _ = try data.nextString(forKey: key)
        return T()
    }
    
    private func addStringType<T: CSVInitializable>(_ type: T.Type, forKey key: Key) throws -> String {
        types.csvTypes.append(type.csvType)
        return try data.nextString(forKey: key)
    }
    
    func decodeNil(forKey key: Key) throws -> Bool {
        _ = try data.nextString(forKey: key)
        return false
    }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try addType(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try addStringType(type, forKey: key) }
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
        return try data.decode(type, forKey: key, decoder: decoder)
    }

    private func addTypeIfPresent<T: CSVInitializable>(_ type: T.Type) -> T {
        types.csvTypes.append(type.csvType)
        _ = data.nextStringIfPresent()
        return T()
    }
    
    private func addStringTypeIfPresent<T: CSVInitializable>(_ type: T.Type) -> String? {
        types.csvTypes.append(type.csvType)
        return data.nextStringIfPresent()
    }
    
    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { addTypeIfPresent(type) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { addStringTypeIfPresent(type) }
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
        KeyedDecodingContainer(TypeKeyedDecoding<NestedKey>(decoder: decoder))
    }
    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedDecodingContainer { TypeUnkeyedDecoding(decoder: decoder) }
    func superDecoder() throws -> any Decoder { try superDecoder(forKey: Key(stringValue: "super")!) }
    func superDecoder(forKey key: Key) throws -> any Decoder { decoder }
}

fileprivate struct TypeUnkeyedDecoding: UnkeyedDecodingContainer {
    private let decoder: TypeRowsDecoder
    var codingPath: [CodingKey] = []
    var count: Int? { return data.rowCount }
    var isAtEnd: Bool { return data.currentRowIndex >= data.rowCount }
    var currentIndex: Int { return data.currentRowIndex }

    init(decoder: TypeRowsDecoder) { self.decoder = decoder }

    var types: DataTypes { decoder.types }
    var data: RowCollection<[[String?]]> { types.data }

    private func checkEnd() throws {
        if isAtEnd {
            throw CSVDecodingError.isAtEnd(rowNumber: data.rowNumber)
        }
    }

    private func addType<T: CSVInitializable>(_ type: T.Type) throws -> T {
        try data.nextRow()
        types.csvTypes.append(type.csvType)
        _ = try data.nextString()
        return T()
    }
    
    private func addStringType<T: CSVInitializable>(_ type: T.Type) throws -> String {
        try data.nextRow()
        types.csvTypes.append(type.csvType)
        return try data.nextString()
    }
    
    mutating func decodeNil() throws -> Bool {
        try data.nextRow()
        _ = try data.nextString()
        return false
    }
    
    mutating func decode(_ type: Bool.Type   ) throws -> Bool    { try addType(type) }
    mutating func decode(_ type: String.Type ) throws -> String  { try addStringType(type) }
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
        try data.nextRow()
        return try data.decode(type, decoder: decoder)
    }

    private func addTypeIfPresent<T: CSVInitializable>(_ type: T.Type) -> T? {
        guard data.nextRowIfPresent() else { return nil }
        types.csvTypes.append(type.csvType)
        _ = data.nextStringIfPresent()
        return T()
    }
    
    private func addStringTypeIfPresent<T: CSVInitializable>(_ type: T.Type) -> String? {
        guard data.nextRowIfPresent() else { return nil }
        types.csvTypes.append(type.csvType)
        return data.nextStringIfPresent()
    }
    
    mutating func decodeIfPresent(_ type: Bool.Type   ) throws -> Bool?    { addTypeIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type ) throws -> String?  { addStringTypeIfPresent(type) }
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
        try data.nextRow()
        _ = addTypeIfPresent(String.self)
        return nil
    }

    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        try checkEnd()
        return KeyedDecodingContainer(TypeKeyedDecoding<NestedKey>(decoder: decoder))
    }

    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw CSVDecodingError.nestedContainer(rowNumber: data.rowNumber)
    }

    mutating func superDecoder() throws -> Decoder { decoder }
}

fileprivate struct TypeSingleValueDecoding: SingleValueDecodingContainer {
    private let decoder: TypeRowsDecoder
    private let index: RowCollection<[[String?]]>.ValueIndex
    var codingPath: [CodingKey] = []

    init(decoder: TypeRowsDecoder) {
        self.decoder = decoder
        self.index = decoder.types.data.getValueIndex()
    }

    var types: DataTypes { decoder.types }
    var data: RowCollection<[[String?]]> { types.data }

    private func addType<T: CSVInitializable>(_ type: T.Type) throws -> T {
        try data.checkValueIndex(index)
        types.csvTypes.append(type.csvType)
        _ = try data.nextString()
        return T()
    }
    
    private func addStringType<T: CSVInitializable>(_ type: T.Type) throws -> String {
        try data.checkValueIndex(index)
        types.csvTypes.append(type.csvType)
        return try data.nextString()
    }
    
    func decodeNil() -> Bool {
        _ = data.nextStringIfPresent()
        return false
    }
    
    func decode(_ type: Bool.Type   ) throws -> Bool    { try addType(type) }
    func decode(_ type: String.Type ) throws -> String  { try addStringType(type) }
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
        return try data.decode(type, decoder: decoder)
    }
}
