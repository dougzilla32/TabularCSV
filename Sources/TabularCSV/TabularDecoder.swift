//
//  TabularDecoder.swift
//  TabularCSV
//
//  Created by Doug on 12/5/24.
//  Based partially on: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

import TabularData

public typealias DataFrameDecoder = TabularDecoder<DataFrame.Rows>

public typealias StringDecoder = TabularDecoder<[[String?]]>

typealias StringRowsDecoder = TabularRowsDecoder<[[String?]]>

public struct TabularDecoder<Rows: DataRows> {
    private let options: ReadingOptions
    
    public init(options: ReadingOptions) { self.options = options }
    
    public func decode<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        rowPermutation: [Int?]? = nil) throws -> T
    {
        try T(from: TabularRowsDecoder(rows: rows, transform: .init(rowPermutation), options: options))
    }

    func decodeWithHeaderAndTypes<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        header: [String]?) throws -> (value: T, headerAndTypes: [HeaderAndType])
    {
        let decoder = TabularRowsDecoder(rows: rows, transform: .init(header), options: options)
        let value = try T(from: decoder)
        return (value: value, headerAndTypes: decoder.data.headerAndTypes!)
    }
}

struct TabularRowsDecoder<Rows: DataRows>: Decoder {
    fileprivate let data: RowCollection<Rows>
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    init(rows: Rows, transform: RowTransform, options: ReadingOptions) {
        self.data = RowCollection(rows: rows, transform: transform, options: options)
    }
    
    static func singleton(rows: Rows, options: ReadingOptions) throws -> TabularRowsDecoder<Rows> {
        let decoder = TabularRowsDecoder(rows: rows, transform: .none, options: options)
        try decoder.data.nextRow()
        return decoder
    }
        
    init(rows: Rows, options: ReadingOptions) {
        self.data = RowCollection(rows: rows, transform: .none, options: options)
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

fileprivate struct TabularKeyedDecoding<Key: CodingKey, Rows: DataRows>: KeyedDecodingContainerProtocol {
    private let decoder: TabularRowsDecoder<Rows>
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { true }
    
    init(decoder: TabularRowsDecoder<Rows>) { self.decoder = decoder }
    
    var data: RowCollection<Rows> { decoder.data }

    func decodeNil(forKey key: Key) throws -> Bool {
        try data.decodeNext(String.self, forKey: key) == "nil"
    }
    
    func decode(_ type: Bool.Type,    forKey key: Key) throws -> Bool    { try data.decodeNext(type, forKey: key) }
    func decode(_ type: String.Type,  forKey key: Key) throws -> String  { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Double.Type,  forKey key: Key) throws -> Double  { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Float.Type,   forKey key: Key) throws -> Float   { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Int.Type,     forKey key: Key) throws -> Int     { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Int8.Type,    forKey key: Key) throws -> Int8    { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Int16.Type,   forKey key: Key) throws -> Int16   { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Int32.Type,   forKey key: Key) throws -> Int32   { try data.decodeNext(type, forKey: key) }
    func decode(_ type: Int64.Type,   forKey key: Key) throws -> Int64   { try data.decodeNext(type, forKey: key) }
    func decode(_ type: UInt.Type,    forKey key: Key) throws -> UInt    { try data.decodeNext(type, forKey: key) }
    func decode(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8   { try data.decodeNext(type, forKey: key) }
    func decode(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16  { try data.decodeNext(type, forKey: key) }
    func decode(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32  { try data.decodeNext(type, forKey: key) }
    func decode(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64  { try data.decodeNext(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: UInt128.Type, forKey key: Key) throws -> UInt128 { try data.decodeNext(type, forKey: key) }

    func decode<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        try data.decodeNext(type, forKey: key, decoder: decoder)
    }

    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { data.decodeNextIfPresent(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decodeIfPresent(_ type: UInt128.Type, forKey key: Key) throws -> UInt128? { data.decodeNextIfPresent(type, forKey: key) }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        try data.decodeNextIfPresent(type, forKey: key, decoder: decoder)
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

fileprivate struct TabularUnkeyedDecoding<Rows: DataRows>: UnkeyedDecodingContainer {
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
    
    private func decodeNext<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        try data.nextRow()
        return try data.decodeNext(type)
    }
    
    mutating func decodeNil() throws -> Bool {
        try data.nextRow()
        return try decodeNext(String.self) == "nil"
    }
    
    mutating func decode(_ type: Bool.Type    ) throws -> Bool    { try decodeNext(type) }
    mutating func decode(_ type: String.Type  ) throws -> String  { try decodeNext(type) }
    mutating func decode(_ type: Double.Type  ) throws -> Double  { try decodeNext(type) }
    mutating func decode(_ type: Float.Type   ) throws -> Float   { try decodeNext(type) }
    mutating func decode(_ type: Int.Type     ) throws -> Int     { try decodeNext(type) }
    mutating func decode(_ type: Int8.Type    ) throws -> Int8    { try decodeNext(type) }
    mutating func decode(_ type: Int16.Type   ) throws -> Int16   { try decodeNext(type) }
    mutating func decode(_ type: Int32.Type   ) throws -> Int32   { try decodeNext(type) }
    mutating func decode(_ type: Int64.Type   ) throws -> Int64   { try decodeNext(type) }
    mutating func decode(_ type: UInt.Type    ) throws -> UInt    { try decodeNext(type) }
    mutating func decode(_ type: UInt8.Type   ) throws -> UInt8   { try decodeNext(type) }
    mutating func decode(_ type: UInt16.Type  ) throws -> UInt16  { try decodeNext(type) }
    mutating func decode(_ type: UInt32.Type  ) throws -> UInt32  { try decodeNext(type) }
    mutating func decode(_ type: UInt64.Type  ) throws -> UInt64  { try decodeNext(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decode(_ type: UInt128.Type ) throws -> UInt128 { try decodeNext(type) }

    mutating func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try data.nextRow()
        return try data.decodeNext(type, decoder: decoder)
    }

    private func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type) -> T? {
        guard data.nextRowIfPresent() else { return nil }
        return data.decodeNextIfPresent(type)
    }

    mutating func decodeIfPresent(_ type: Bool.Type    ) throws -> Bool?    { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type  ) throws -> String?  { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Double.Type  ) throws -> Double?  { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Float.Type   ) throws -> Float?   { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int.Type     ) throws -> Int?     { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int8.Type    ) throws -> Int8?    { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int16.Type   ) throws -> Int16?   { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int32.Type   ) throws -> Int32?   { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int64.Type   ) throws -> Int64?   { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt.Type    ) throws -> UInt?    { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt8.Type   ) throws -> UInt8?   { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt16.Type  ) throws -> UInt16?  { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt32.Type  ) throws -> UInt32?  { decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt64.Type  ) throws -> UInt64?  { decodeNextIfPresent(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decodeIfPresent(_ type: UInt128.Type ) throws -> UInt128? { decodeNextIfPresent(type) }

    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? {
        try data.nextRow()
        return try data.decodeNextIfPresent(type, decoder: decoder)
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

fileprivate struct TabularSingleValueDecoding<Rows: DataRows>: SingleValueDecodingContainer {
    private let decoder: TabularRowsDecoder<Rows>
    private let index: RowCollection<Rows>.ValueIndex
    var codingPath: [CodingKey] = []

    init(decoder: TabularRowsDecoder<Rows>) {
        self.decoder = decoder
        self.index = decoder.data.getValueIndex()
    }
    
    var data: RowCollection<Rows> { decoder.data }
    
    private func decodeNext<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        try data.checkValueIndex(index)
        return try data.decodeNext(type)
    }

    func decodeNil() -> Bool {
        guard data.checkValueIndexIfPresent(index) else { return false }
        return data.decodeNextIfPresent(String.self) == "nil"
    }
    
    func decode(_ type: Bool.Type   ) throws -> Bool    { try decodeNext(type) }
    func decode(_ type: String.Type ) throws -> String  { try decodeNext(type) }
    func decode(_ type: Double.Type ) throws -> Double  { try decodeNext(type) }
    func decode(_ type: Float.Type  ) throws -> Float   { try decodeNext(type) }
    func decode(_ type: Int.Type    ) throws -> Int     { try decodeNext(type) }
    func decode(_ type: Int8.Type   ) throws -> Int8    { try decodeNext(type) }
    func decode(_ type: Int16.Type  ) throws -> Int16   { try decodeNext(type) }
    func decode(_ type: Int32.Type  ) throws -> Int32   { try decodeNext(type) }
    func decode(_ type: Int64.Type  ) throws -> Int64   { try decodeNext(type) }
    func decode(_ type: UInt.Type   ) throws -> UInt    { try decodeNext(type) }
    func decode(_ type: UInt8.Type  ) throws -> UInt8   { try decodeNext(type) }
    func decode(_ type: UInt16.Type ) throws -> UInt16  { try decodeNext(type) }
    func decode(_ type: UInt32.Type ) throws -> UInt32  { try decodeNext(type) }
    func decode(_ type: UInt64.Type ) throws -> UInt64  { try decodeNext(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decode(_ type: UInt128.Type) throws -> UInt128 { try decodeNext(type) }

    func decode<T: Decodable>(_ type: T.Type) throws -> T {
        try data.checkValueIndex(index)
        return try data.decodeNext(type, decoder: decoder)
    }
}
