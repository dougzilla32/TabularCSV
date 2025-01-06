//
//  TabularEncoder.swift
//  TabularCSV
//
//  Created by Doug on 12/18/24.
//

import TabularData

public typealias DataFrameEncoder = TabularEncoder<AnyColumnMatrix>

public typealias StringEncoder = TabularEncoder<StringMatrix>

public struct TabularEncoder<Values: DataMatrix> {
    private let options: WritingOptions

    public init(options: WritingOptions) {
        self.options = options
    }

    public func encode<T: Encodable & Collection>(
        _ value: T,
        header: [String]?) throws -> [Values.VectorType]
    {
        let encoder = TabularRowsEncoder<Values>(header: header, numRows: value.count, options: options)
        try encoder.encode(value)
        return encoder.data.matrix.vectors
    }
}

fileprivate struct TabularRowsEncoder<Matrix: DataMatrix>: Encoder {
    fileprivate var data: VectorCollection<Matrix>
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey : Any] = [:]

    init(header: [String]?, numRows: Int, options: WritingOptions) {
        data = VectorCollection<Matrix>(header: header, numRows: numRows, options: options)
    }
    
    init(data: VectorCollection<Matrix>) { self.data = data }
    
    func encode<T: Encodable>(_ value: T) throws {
        try value.encode(to: self)
        data.columnIndex = 0
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(TabularKeyedEncoding<Key, Matrix>(encoder: self))
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        TabularUnkeyedEncoding(encoder: self)
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        TabularSingleValueEncoding(encoder: self)
    }
}

fileprivate struct TabularKeyedEncoding<Key: CodingKey, Matrix: DataMatrix>: KeyedEncodingContainerProtocol {
    private let encoder: TabularRowsEncoder<Matrix>
    var codingPath: [CodingKey] = []
    
    init(encoder: TabularRowsEncoder<Matrix>) { self.encoder = encoder }
    
    var data: VectorCollection<Matrix> { encoder.data }
    
    mutating func encodeNil(                forKey key: Key) throws { data.encodeNext("nil") }
    mutating func encode(_ value: Bool,     forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: String,   forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Double,   forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Float,    forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Int,      forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Int8,     forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Int16,    forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Int32,    forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: Int64,    forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt,     forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt8,    forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt16,   forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt32,   forKey key: Key) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt64,   forKey key: Key) throws { data.encodeNext(value) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128,  forKey key: Key) throws { data.encodeNext(value) }

    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        try data.encode(value, encoder: encoder)
    }
    
    mutating func encodeIfPresent(_ value: Bool?,    forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: String?,  forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Double?,  forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Float?,   forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Int?,     forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Int8?,    forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Int16?,   forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Int32?,   forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: Int64?,   forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: UInt?,    forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: UInt8?,   forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: UInt16?,  forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: UInt32?,  forKey key: Key) throws { data.encodeNextIfPresent(value) }
    mutating func encodeIfPresent(_ value: UInt64?,  forKey key: Key) throws { data.encodeNextIfPresent(value) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encodeIfPresent(_ value: UInt128?, forKey key: Key) throws { data.encodeNextIfPresent(value) }

    mutating func encodeIfPresent<T: Encodable>(_ value: T?, forKey key: Key) throws {
        try data.encodeIfPresent(value, encoder: encoder)
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(TabularKeyedEncoding<NestedKey, Matrix>(encoder: encoder))
    }
    
    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        TabularUnkeyedEncoding(encoder: encoder)
    }
    
    mutating func superEncoder() -> Encoder { superEncoder(forKey: Key(stringValue: "super")!) }

    mutating func superEncoder(forKey key: Key) -> Encoder { encoder }
}

fileprivate struct TabularUnkeyedEncoding<Matrix: DataMatrix>: UnkeyedEncodingContainer {
    private let encoder: TabularRowsEncoder<Matrix>
    var codingPath: [CodingKey] = []
    var count: Int { data.matrix.numRows }

    init(encoder: TabularRowsEncoder<Matrix>) { self.encoder = encoder }
    
    var data: VectorCollection<Matrix> { encoder.data }
    
    private func encodeNext<T: CSVPrimitive>(_ value: T) throws {
        data.encodeNext(value)
        data.nextRow()
    }
    
    mutating func encodeNil()             throws { data.encodeNext("nil") }
    mutating func encode(_ value: Bool)   throws { data.encodeNext(value) }
    mutating func encode(_ value: String) throws { data.encodeNext(value) }
    mutating func encode(_ value: Double) throws { data.encodeNext(value) }
    mutating func encode(_ value: Float)  throws { data.encodeNext(value) }
    mutating func encode(_ value: Int)    throws { data.encodeNext(value) }
    mutating func encode(_ value: Int8)   throws { data.encodeNext(value) }
    mutating func encode(_ value: Int16)  throws { data.encodeNext(value) }
    mutating func encode(_ value: Int32)  throws { data.encodeNext(value) }
    mutating func encode(_ value: Int64)  throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt)   throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt8)  throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt16) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt32) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt64) throws { data.encodeNext(value) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128) throws { data.encodeNext(value) }

    mutating func encode<T: Encodable>(_ value: T) throws {
        try data.encode(value, encoder: encoder)
        data.nextRow()
    }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(TabularKeyedEncoding<NestedKey, Matrix>(encoder: encoder))
    }

    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        TabularUnkeyedEncoding<Matrix>(encoder: encoder)
    }

    mutating func superEncoder() -> Encoder {
        encoder
    }
}

fileprivate struct TabularSingleValueEncoding<Matrix: DataMatrix>: SingleValueEncodingContainer {
    private let encoder: TabularRowsEncoder<Matrix>
    var codingPath: [CodingKey] = []

    init(encoder: TabularRowsEncoder<Matrix>) { self.encoder = encoder }

    var data: VectorCollection<Matrix> { encoder.data }

    mutating func encodeNil()             throws { data.encodeNext("nil") }
    mutating func encode(_ value: Bool)   throws { data.encodeNext(value) }
    mutating func encode(_ value: String) throws { data.encodeNext(value) }
    mutating func encode(_ value: Double) throws { data.encodeNext(value) }
    mutating func encode(_ value: Float)  throws { data.encodeNext(value) }
    mutating func encode(_ value: Int)    throws { data.encodeNext(value) }
    mutating func encode(_ value: Int8)   throws { data.encodeNext(value) }
    mutating func encode(_ value: Int16)  throws { data.encodeNext(value) }
    mutating func encode(_ value: Int32)  throws { data.encodeNext(value) }
    mutating func encode(_ value: Int64)  throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt)   throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt8)  throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt16) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt32) throws { data.encodeNext(value) }
    mutating func encode(_ value: UInt64) throws { data.encodeNext(value) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128)throws { data.encodeNext(value) }

    mutating func encode<T: Encodable>(_ value: T) throws {
        try data.encode(value, encoder: encoder)
    }
}
