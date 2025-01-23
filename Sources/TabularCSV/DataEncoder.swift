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
        header: [String]?,
        rowPermutation: [Int?]? = nil) throws -> [Values.VectorType]
    {
        let encoder = DataEncoder<Values>(header: header, numRows: value.count, transform: .init(rowPermutation), options: options)
        try encoder.encode(value)
        return encoder.data.matrix.getVectors()
    }
    
    func encodeWithHeaderAndTypes<T: Encodable & Collection>(
        _ value: T,
        header: [String]?) throws -> (data: [Values.VectorType], fields: OrderedSet<CSVField>)
    {
        let encoder = DataEncoder<Values>(header: header, numRows: value.count, transform: .map(nil), options: options)
        try encoder.encode(value)
        return (data: encoder.data.matrix.getVectors(), fields: encoder.data.fields!.immutableCopy())
    }
}

fileprivate struct DataEncoder<Matrix: DataMatrix>: Encoder {
    fileprivate var data: VectorCollection<Matrix>
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey : Any] = [:]

    init(header: [String]?, numRows: Int, transform: RowTransform, options: WritingOptions) {
        data = VectorCollection<Matrix>(header: header, numRows: numRows, transform: transform, options: options)
    }
    
    init(data: VectorCollection<Matrix>) { self.data = data }
    
    func encode<T: Encodable>(_ value: T) throws {
        try value.encode(to: self)
        data.currentColumnIndex = 0
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(DataKeyedEncoding<Key, Matrix>(encoder: self))
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        DataUnkeyedEncoding(encoder: self)
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        DataSingleValueEncoding(encoder: self)
    }
}

fileprivate struct DataKeyedEncoding<Key: CodingKey, Matrix: DataMatrix>: KeyedEncodingContainerProtocol {
    private let encoder: DataEncoder<Matrix>
    var codingPath: [CodingKey] = []
    
    init(encoder: DataEncoder<Matrix>) { self.encoder = encoder }
    
    var data: VectorCollection<Matrix> { encoder.data }
    
    mutating func encodeNil(                forKey key: Key) throws { data.encodeNext("nil", forKey: key) }
    mutating func encode(_ value: Bool,     forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: String,   forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Double,   forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Float,    forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Int,      forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Int8,     forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Int16,    forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Int32,    forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: Int64,    forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: UInt,     forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: UInt8,    forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: UInt16,   forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: UInt32,   forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    mutating func encode(_ value: UInt64,   forKey key: Key) throws { data.encodeNext(value, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encode(_ value: UInt128,  forKey key: Key) throws { data.encodeNext(value, forKey: key) }

    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws {
        try data.encodeNext(value, forKey: key, encoder: encoder)
    }
    
    mutating func encodeIfPresent(_ value: Bool?,    forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: String?,  forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Double?,  forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Float?,   forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int?,     forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int8?,    forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int16?,   forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int32?,   forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: Int64?,   forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt?,    forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt8?,   forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt16?,  forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt32?,  forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    mutating func encodeIfPresent(_ value: UInt64?,  forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }
    @available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
    mutating func encodeIfPresent(_ value: UInt128?, forKey key: Key) throws { data.encodeNextIfPresent(value, forKey: key) }

    mutating func encodeIfPresent<T: Encodable>(_ value: T?, forKey key: Key) throws {
        try data.encodeNextIfPresent(value, forKey: key, encoder: encoder)
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
    
    mutating func superEncoder() -> Encoder { superEncoder(forKey: Key(stringValue: "super")!) }

    mutating func superEncoder(forKey key: Key) -> Encoder { encoder }
}

fileprivate struct DataUnkeyedEncoding<Matrix: DataMatrix>: UnkeyedEncodingContainer {
    private let encoder: DataEncoder<Matrix>
    var codingPath: [CodingKey] = []
    var count: Int { data.matrix.numRows }

    init(encoder: DataEncoder<Matrix>) { self.encoder = encoder }
    
    var data: VectorCollection<Matrix> { encoder.data }
    
    private func encodeNext<T: CSVPrimitive>(_ value: T) throws {
        data.nextRow()
        data.encodeNext(value)
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
        data.nextRow()
        try data.encodeNext(value, encoder: encoder)
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
    private let encoder: DataEncoder<Matrix>
    var codingPath: [CodingKey] = []

    init(encoder: DataEncoder<Matrix>) { self.encoder = encoder }

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
        try data.encodeNext(value, encoder: encoder)
    }
}
