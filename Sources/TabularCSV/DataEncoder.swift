//
//  TypedEncoder.swift
//  TabularCSV
//
//  Created by Doug on 12/18/24.
//

import TabularData

public typealias DataFrameEncoder = TypedEncoder<AnyColumnMatrix>

public typealias StringEncoder = TypedEncoder<StringMatrix>

public struct TypedEncoder<Values: DataMatrix> {
    private let options: WritingOptions
    
    public init(options: WritingOptions) {
        self.options = options
    }
    
    public func encode<T: Encodable & Collection>(
        _ value: T,
        header: [String]) throws -> [Values.VectorType]
    {
        let headerIndicies = OrderedDictionary(uniqueKeysWithValues: header.enumerated().map { ($1, $0) })
        let encoder = DataEncoder<Values>(header: headerIndicies, numRows: value.count, options: options, introspect: false)
        try value.encode(to: encoder)
        return encoder.matrix.vectors
    }
    
    func encodeWithHeaderAndTypes<T: Encodable & Collection>(
        _ value: T,
        header: [String]?) throws -> (data: [Values.VectorType], fields: OrderedSet<CSVField>)
    {
        let headerIndicies: OrderedDictionary<String, Int>?
        if let header {
            headerIndicies = OrderedDictionary(uniqueKeysWithValues: header.enumerated().map { ($1, $0) })
        } else {
            headerIndicies = nil
        }

        let encoder = DataEncoder<Values>(header: headerIndicies, numRows: value.count, options: options, introspect: true)
        try value.encode(to: encoder)
        return (data: encoder.matrix.vectors, fields: encoder.fields!)
    }
}

fileprivate class DataEncoder<Matrix: DataMatrix>: Encoder {
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey : Any] = [:]

    var matrix: Matrix
    let header: OrderedDictionary<String, Int>?
    let options: WritingOptions
    var currentKey: CodingKey? = nil
    var currentColumnIndex = 0
    var fields: OrderedSet<CSVField>?

    init(header: OrderedDictionary<String, Int>?, numRows: Int, options: WritingOptions, introspect: Bool) {
        self.matrix = Matrix(header: header, numRows: numRows)
        self.header = header
        self.options = options

        if introspect {
            fields = OrderedSet<CSVField>()
        }
    }
    
    func nextRow() {
        matrix.nextRow()
        currentColumnIndex = 0
    }
    
    func index(forKey key: CodingKey?) -> Int? {
        guard let key else { return 0 }
        let index: Int
        if let header {
            guard let mappedIndex = header[key.stringValue] else {
                return nil
            }
            index = mappedIndex
        } else {
            index = currentColumnIndex
        }
        return index
    }
    
    func encodeNil(forKey key: CodingKey?) {
        defer { currentKey = nil }
        currentKey = key

        guard let index = index(forKey: key) else { return }
        matrix.encode(nil as String?, index: index)
    }

    func encode<T: Encodable>(_ value: T, forKey key: CodingKey) throws {
        defer { currentKey = nil }
        currentKey = key

        guard let _ = field(key, type: .string) else { return }

        try encodeValue(value)
    }
    
    func encodeIfPresent<T: Encodable>(_ value: T?, forKey key: CodingKey) throws {
        defer { currentKey = nil }
        currentKey = key

        guard let index = field(key, type: .string) else { return }

        guard let value else {
            matrix.encode(nil as String?, index: index)
            return
        }
        
        try encodeValue(value)
    }
    
    @inline(__always)
    private func encodeValue<T: Encodable>(_ value: T) throws {
        if let formatter = options.formatterForType(T.self) {
            try formatter(value).encode(to: self)
        } else {
            try value.encode(to: self)
        }
    }
    
    func encodePrim<T: CSVPrimitive>(_ value: T, forKey key: CodingKey?) {
        encodePrimIfPresent(value, forKey: key)
    }

    func encodePrimIfPresent<T: CSVPrimitive>(_ value: T?, forKey key: CodingKey?) {
        defer { currentKey = nil }
        currentKey = key

        guard let index = field(key, type: T.csvType) else { return }
        matrix.encode(value, index: index)
    }
    
    @inline(__always)
    private func field(_ key: CodingKey?, type: CSVType) -> Int? {
        guard let index = index(forKey: key) else { return nil }
        fields?.add(.init(name: key?.stringValue ?? "Column 0", type: type))
        currentColumnIndex += 1
        return index
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(DataKeyedEncoding<Key, Matrix>(encoder: self))
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        DataUnkeyedEncoding(encoder: self)
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        DataSingleValueEncoding(key: currentKey, encoder: self)
    }
}

fileprivate struct DataKeyedEncoding<Key: CodingKey, Matrix: DataMatrix>: KeyedEncodingContainerProtocol {
    private var encoder: DataEncoder<Matrix>
    var codingPath: [CodingKey] = []
    
    init(encoder: DataEncoder<Matrix>) { self.encoder = encoder }
    
    mutating func encodeNil(forKey key: Key) throws {
        encoder.encodeNil(forKey: key)
    }

    mutating func encode(_ value: Bool,     forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: String,   forKey key: Key) throws { encodeString(value, forKey: key) }
    mutating func encode(_ value: Double,   forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: Float,    forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: Int,      forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: Int8,     forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: Int16,    forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: Int32,    forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: Int64,    forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: UInt,     forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: UInt8,    forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: UInt16,   forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: UInt32,   forKey key: Key) throws { encodePrim(value, forKey: key) }
    mutating func encode(_ value: UInt64,   forKey key: Key) throws { encodePrim(value, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128,  forKey key: Key) throws { encodePrim(value, forKey: key) }

    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        try encoder.encode(value, forKey: key)
    }
    
    mutating func encodeIfPresent(_ value: Bool?,    forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: String?,  forKey key: Key) throws { encodeStringIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Double?,  forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Float?,   forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int?,     forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int8?,    forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int16?,   forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int32?,   forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int64?,   forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt?,    forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt8?,   forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt16?,  forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt32?,  forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt64?,  forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encodeIfPresent(_ value: UInt128?, forKey key: Key) throws { encodePrimIfPresent(value, forKey: key) }

    mutating func encodeIfPresent<T: Encodable>(_ value: T?, forKey key: Key) throws {
        try encoder.encodeIfPresent(value, forKey: key)
    }
    
    private mutating func encodePrim<T: CSVPrimitive>(_ value: T, forKey key: CodingKey) {
        encoder.encodePrim(value, forKey: key)
    }
    
    private mutating func encodePrimIfPresent<T: CSVPrimitive>(_ value: T?, forKey key: CodingKey) {
        encoder.encodePrimIfPresent(value, forKey: key)
    }
    
    private mutating func encodeString(_ value: String, forKey key: CodingKey) {
        encoder.encodePrim(value, forKey: key)
    }
    
    private mutating func encodeStringIfPresent(_ value: String?, forKey key: CodingKey) {
        encoder.encodePrimIfPresent(value, forKey: key)
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(DataKeyedEncoding<NestedKey, Matrix>(encoder: encoder))
    }
    
    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        DataUnkeyedEncoding(encoder: encoder)
    }
    
    mutating func superEncoder() -> Encoder { encoder }

    mutating func superEncoder(forKey key: Key) -> Encoder { encoder }
}

fileprivate struct DataUnkeyedEncoding<Matrix: DataMatrix>: UnkeyedEncodingContainer {
    private var encoder: DataEncoder<Matrix>
    var codingPath: [CodingKey] = []
    var count: Int { encoder.matrix.numRows }

    init(encoder: DataEncoder<Matrix>) { self.encoder = encoder }
    
    mutating func encodeNil() throws {
        throw DataEncodingError.invalidUnkeyedValue(nil as String?)
    }
    
    mutating func encode(_ value: Bool)   throws { try encodePrim(value) }
    mutating func encode(_ value: String) throws { try encodeString(value) }
    mutating func encode(_ value: Double) throws { try encodePrim(value) }
    mutating func encode(_ value: Float)  throws { try encodePrim(value) }
    mutating func encode(_ value: Int)    throws { try encodePrim(value) }
    mutating func encode(_ value: Int8)   throws { try encodePrim(value) }
    mutating func encode(_ value: Int16)  throws { try encodePrim(value) }
    mutating func encode(_ value: Int32)  throws { try encodePrim(value) }
    mutating func encode(_ value: Int64)  throws { try encodePrim(value) }
    mutating func encode(_ value: UInt)   throws { try encodePrim(value) }
    mutating func encode(_ value: UInt8)  throws { try encodePrim(value) }
    mutating func encode(_ value: UInt16) throws { try encodePrim(value) }
    mutating func encode(_ value: UInt32) throws { try encodePrim(value) }
    mutating func encode(_ value: UInt64) throws { try encodePrim(value) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128) throws { try encodePrim(value) }

    mutating func encode<T: Encodable>(_ value: T) throws {
        encoder.nextRow()
        try value.encode(to: encoder)
    }
    
    private mutating func encodePrim<T: CSVPrimitive>(_ value: T) throws {
        throw DataEncodingError.invalidUnkeyedValue(value)
    }
    
    private mutating func encodeString<T: CSVPrimitive>(_ value: T) throws {
        throw DataEncodingError.invalidUnkeyedValue(value)
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(DataKeyedEncoding<NestedKey, Matrix>(encoder: encoder))
    }

    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        DataUnkeyedEncoding<Matrix>(encoder: encoder)
    }

    mutating func superEncoder() -> Encoder {
        encoder
    }
}

fileprivate struct DataSingleValueEncoding<Matrix: DataMatrix>: SingleValueEncodingContainer {
    var userInfo: [CodingUserInfoKey : Any] = [:]
    
    private let key: CodingKey?
    private let encoder: DataEncoder<Matrix>
    var codingPath: [CodingKey] = []
    
    init(key: CodingKey? = nil, encoder: DataEncoder<Matrix>) {
        self.key = key
        self.encoder = encoder
    }

    mutating func encodeNil() throws {
        encoder.encodeNil(forKey: key)
    }
    
    mutating func encode(_ value: Bool)   throws { encodePrim(value) }
    mutating func encode(_ value: String) throws { encodeString(value) }
    mutating func encode(_ value: Double) throws { encodePrim(value) }
    mutating func encode(_ value: Float)  throws { encodePrim(value) }
    mutating func encode(_ value: Int)    throws { encodePrim(value) }
    mutating func encode(_ value: Int8)   throws { encodePrim(value) }
    mutating func encode(_ value: Int16)  throws { encodePrim(value) }
    mutating func encode(_ value: Int32)  throws { encodePrim(value) }
    mutating func encode(_ value: Int64)  throws { encodePrim(value) }
    mutating func encode(_ value: UInt)   throws { encodePrim(value) }
    mutating func encode(_ value: UInt8)  throws { encodePrim(value) }
    mutating func encode(_ value: UInt16) throws { encodePrim(value) }
    mutating func encode(_ value: UInt32) throws { encodePrim(value) }
    mutating func encode(_ value: UInt64) throws { encodePrim(value) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128)throws { encodePrim(value) }
    
    mutating func encode<T: Encodable>(_ value: T) throws {
        if let key {
            try encoder.encode(value, forKey: key)
        } else {
            try value.encode(to: encoder)
        }
    }
    
    private mutating func encodePrim<T: CSVPrimitive>(_ value: T) {
        encoder.encodePrim(value, forKey: key)
    }
    
    private mutating func encodeString(_ value: String) {
        encoder.encodePrim(value, forKey: key)
    }
}
