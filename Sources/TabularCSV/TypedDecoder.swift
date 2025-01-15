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
        rowPermutation: [Int?]?) throws -> T
    {
        let data = RowPermutation<Rows, Columns>(rows: rows, columns: columns, permutation: rowPermutation, options: options)
        return try T(from: DataDecoder<Rows, Columns>(data: data))
    }
    
    public func decodeWithMap<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        columns: Columns,
        header: [String]?) throws -> T
    {
        let map = keyIndexMap(header)
        let data = RowMap<Rows, Columns>(rows: rows, columns: columns, map: map, options: options)
        let decoder = DataDecoder<Rows, Columns>(data: data)
        return try T(from: decoder)
    }

    func decodeTypes<T: Decodable & Collection>(
        _ type: T.Type,
        rows: Rows,
        columns: Columns,
        header: [String]?) throws -> (value: T, headerAndTypes: [HeaderAndType])
    {
        var result: (value: T, headerAndTypes: [HeaderAndType])?
        var nilKeys: Set<String> = []
        let map = keyIndexMap(header)

        repeat {
            let data = RowTypes<Rows, Columns>(rows: rows, columns: columns, map: map, nilKeys: nilKeys, options: options)
            let decoder = DataDecoder<Rows, Columns>(data: data)
            
            do {
                let value = try T(from: decoder)
                result = (value: value, headerAndTypes: data.headerAndTypes)
            } catch RowTypeError.nilDecodingError {
                nilKeys = data.nilKeys
            } catch {
                throw error
            }
        } while result == nil

        return result!
    }
    
    private func keyIndexMap(_ header: [String]?) -> [String: Int]? {
        let map: [String: Int]?
        if let header {
            map = Dictionary(uniqueKeysWithValues: header.enumerated().map { ($1, $0) })
        } else {
            map = nil
        }
        return map
    }
}

struct DataDecoder<Rows: DataRows, Columns: DataColumns>: Decoder {
    fileprivate let data: RowCollection
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey: Any] = [:]

    init(data: RowCollection) {
        self.data = data
    }
    
    static func singleton(rows: Rows, columns: Columns, options: ReadingOptions) throws -> DataDecoder<Rows, Columns> {
        let data = RowPermutation<Rows, Columns>(rows: rows, columns: columns, permutation: nil, options: options)
        let decoder = DataDecoder<Rows, Columns>(data: data)
        try decoder.data.nextRow()
        return decoder
    }
        
    public func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedDecodingContainer<Key> {
        KeyedDecodingContainer(DataKeyedDecoding<Key, Rows, Columns>(decoder: self))
    }
    
    public func unkeyedContainer() -> UnkeyedDecodingContainer {
        DataUnkeyedDecoding(decoder: self)
    }
    
    public func singleValueContainer() -> SingleValueDecodingContainer {
        DataSingleValueDecoding(decoder: self)
    }
}

fileprivate struct DataKeyedDecoding<Key: CodingKey, Rows: DataRows, Columns: DataColumns>: KeyedDecodingContainerProtocol {
    private let decoder: DataDecoder<Rows, Columns>
    var codingPath: [CodingKey] = []
    var allKeys: [Key] = []
    func contains(_ key: Key) -> Bool { true }
    
    init(decoder: DataDecoder<Rows, Columns>) { self.decoder = decoder }
    
    var data: RowCollection { decoder.data }

    func decodeNil(forKey key: Key) throws -> Bool {
        try data.decodeNil(forKey: key)
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

    func decodeIfPresent(_ type: Bool.Type,    forKey key: Key) throws -> Bool?    { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: String.Type,  forKey key: Key) throws -> String?  { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Double.Type,  forKey key: Key) throws -> Double?  { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Float.Type,   forKey key: Key) throws -> Float?   { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int.Type,     forKey key: Key) throws -> Int?     { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int8.Type,    forKey key: Key) throws -> Int8?    { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int16.Type,   forKey key: Key) throws -> Int16?   { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int32.Type,   forKey key: Key) throws -> Int32?   { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: Int64.Type,   forKey key: Key) throws -> Int64?   { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt.Type,    forKey key: Key) throws -> UInt?    { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt8.Type,   forKey key: Key) throws -> UInt8?   { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt16.Type,  forKey key: Key) throws -> UInt16?  { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt32.Type,  forKey key: Key) throws -> UInt32?  { try data.decodeNextIfPresent(type, forKey: key) }
    func decodeIfPresent(_ type: UInt64.Type,  forKey key: Key) throws -> UInt64?  { try data.decodeNextIfPresent(type, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    func decodeIfPresent(_ type: UInt128.Type, forKey key: Key) throws -> UInt128? { try data.decodeNextIfPresent(type, forKey: key) }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        try data.decodeNextIfPresent(type, forKey: key, decoder: decoder)
    }
    
    func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) throws -> KeyedDecodingContainer<NestedKey>
    {
        throw DataDecodingError.nestedContainer(forKey: key, rowNumber: data.rowNumber)
    }
    
    func nestedUnkeyedContainer(forKey key: Key) throws -> UnkeyedDecodingContainer {
        throw DataDecodingError.nestedContainer(forKey: key, rowNumber: data.rowNumber)
    }

    func superDecoder() throws -> any Decoder {
        try superDecoder(forKey: Key(stringValue: "super")!)
    }

    func superDecoder(forKey key: Key) throws -> any Decoder {
        decoder
    }
}

fileprivate struct DataUnkeyedDecoding<Rows: DataRows, Columns: DataColumns>: UnkeyedDecodingContainer {
    private let decoder: DataDecoder<Rows, Columns>
    var codingPath: [CodingKey] = []
    var count: Int? { return data.rowCount }
    var isAtEnd: Bool { return data.currentRowIndex >= data.rowCount }
    var currentIndex: Int { return data.currentRowIndex }
    
    init(decoder: DataDecoder<Rows, Columns>) { self.decoder = decoder }
    
    var data: RowCollection { decoder.data }
    
    private func checkEnd() throws {
        if isAtEnd {
            throw DataDecodingError.isAtEnd(rowNumber: data.rowNumber)
        }
    }
    
    private func decodeNext<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        try data.nextRow()
        return try data.decodeNext(type, forKey: nil)
    }
    
    mutating func decodeNil() throws -> Bool {
        try data.nextRow()
        return try data.decodeNil(forKey: nil)
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
        return try data.decodeNext(type, forKey: nil, decoder: decoder)
    }

    private func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type) throws -> T? {
        guard try data.nextRowIfPresent() else { return nil }
        return try data.decodeNextIfPresent(type, forKey: nil)
    }

    mutating func decodeIfPresent(_ type: Bool.Type    ) throws -> Bool?    { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: String.Type  ) throws -> String?  { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Double.Type  ) throws -> Double?  { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Float.Type   ) throws -> Float?   { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int.Type     ) throws -> Int?     { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int8.Type    ) throws -> Int8?    { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int16.Type   ) throws -> Int16?   { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int32.Type   ) throws -> Int32?   { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: Int64.Type   ) throws -> Int64?   { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt.Type    ) throws -> UInt?    { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt8.Type   ) throws -> UInt8?   { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt16.Type  ) throws -> UInt16?  { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt32.Type  ) throws -> UInt32?  { try decodeNextIfPresent(type) }
    mutating func decodeIfPresent(_ type: UInt64.Type  ) throws -> UInt64?  { try decodeNextIfPresent(type) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func decodeIfPresent(_ type: UInt128.Type ) throws -> UInt128? { try decodeNextIfPresent(type) }

    mutating func decodeIfPresent<T: Decodable>(_ type: T.Type) throws -> T? {
        try data.nextRow()
        return try data.decodeNextIfPresent(type, forKey: nil, decoder: decoder)
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> {
        try checkEnd()
        return KeyedDecodingContainer(DataKeyedDecoding<NestedKey, Rows, Columns>(decoder: decoder))
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        throw DataDecodingError.nestedContainer(rowNumber: data.rowNumber)
    }
    
    mutating func superDecoder() throws -> Decoder {
        decoder
    }
}

fileprivate struct DataSingleValueDecoding<Rows: DataRows, Columns: DataColumns>: SingleValueDecodingContainer {
    private let decoder: DataDecoder<Rows, Columns>
    var codingPath: [CodingKey] = []

    init(decoder: DataDecoder<Rows, Columns>) {
        self.decoder = decoder
        decoder.data.singleValueContainer()
    }
    
    var data: RowCollection { decoder.data }
    
    private func decodeNext<T: CSVPrimitive>(_ type: T.Type) throws -> T {
        try decoder.data.singleValueDecode(isDecodeNil: false)
        return try data.decodeNext(type, forKey: nil)
    }

    func decodeNil() -> Bool {
        do {
            try decoder.data.singleValueDecode(isDecodeNil: true)
            return try data.decodeNil(forKey: nil)
        } catch {
            decoder.data.deferredError(error)
            return true
        }
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
        try decoder.data.singleValueDecode(isDecodeNil: false)
        return try data.decodeNext(type, forKey: nil, decoder: decoder)
    }
}
