//
//  DataFrameEncoder.swift
//  TabularCSV
//
//  Created by Doug on 12/18/24.
//

import TabularData

public struct DataFrameEncoder {
    private let options: WritingOptions

    public init(options: WritingOptions) {
        self.options = options
    }

    public func encode<T: Encodable>(_ values: [T], header: [String]?) throws -> [AnyColumn] {
        let dataFrameEncoding = DataFrameEncoding(header: header, numRows: values.count, options: options)
        for value in values {
            try dataFrameEncoding.encode(value)
        }
        return dataFrameEncoding.data.columns
    }
}

fileprivate struct DataFrameEncoding: Encoder {
    fileprivate var data: ColumnCollection
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey : Any] = [:]

    init(header: [String]?, numRows: Int, options: WritingOptions) {
        data = ColumnCollection(header: header, numRows: numRows, options: options)
    }
    
    init(data: ColumnCollection) { self.data = data }
    
    func encode<T: Encodable>(_ value: T) throws {
        try value.encode(to: self)
        data.columnIndex = 0
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(DataFrameKeyedEncoding<Key>(encoding: self))
    }
    func unkeyedContainer() -> UnkeyedEncodingContainer { DataFrameUnkeyedEncoding(encoding: self) }
    func singleValueContainer() -> SingleValueEncodingContainer { DataFrameSingleValueEncoding(encoding: self) }
}

fileprivate struct DataFrameKeyedEncoding<Key: CodingKey>: KeyedEncodingContainerProtocol {
    private let encoding: DataFrameEncoding
    var codingPath: [CodingKey] = []
    
    init(encoding: DataFrameEncoding) { self.encoding = encoding }
    
    var data: ColumnCollection { encoding.data }
    
    mutating func encodeNil(                forKey key: Key) throws { data.encode("nil") }
    mutating func encode(_ value: Bool,     forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: String,   forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Double,   forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Float,    forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Int,      forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Int8,     forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Int16,    forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Int32,    forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Int64,    forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: UInt,     forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: UInt8,    forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: UInt16,   forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: UInt32,   forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: UInt64,   forKey key: Key) throws { data.encode(value) }
    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws { try data.encode(T.self, value: value, rowNumber: 0, encoding: encoding) }
    
    mutating func encodeIfPresent(_ value: Bool?,    forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: String?,  forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Double?,  forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Float?,   forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Int?,     forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Int8?,    forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Int16?,   forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Int32?,   forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: Int64?,   forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: UInt?,    forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: UInt8?,   forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: UInt16?,  forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: UInt32?,  forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent(_ value: UInt64?,  forKey key: Key) throws { data.encode(value) }
    mutating func encodeIfPresent<T: Encodable>(_ value: T?, forKey key: Key) throws { try value.encode(to: DataFrameEncoding(data: data)) }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(DataFrameKeyedEncoding<NestedKey>(encoding: encoding))
    }
    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer { DataFrameUnkeyedEncoding(encoding: encoding) }
    mutating func superEncoder() -> Encoder { superEncoder(forKey: Key(stringValue: "super")!) }
    mutating func superEncoder(forKey key: Key) -> Encoder { encoding }
}

fileprivate struct DataFrameUnkeyedEncoding: UnkeyedEncodingContainer {
    private let encoding: DataFrameEncoding
    var codingPath: [CodingKey] = []
    var count: Int { data.header?.count ?? 0 }

    init(encoding: DataFrameEncoding) { self.encoding = encoding }
    
    var data: ColumnCollection { encoding.data }
    
    mutating func encodeNil()             throws { data.encode("nil") }
    mutating func encode(_ value: Bool)   throws { data.encode(value) }
    mutating func encode(_ value: String) throws { data.encode(value) }
    mutating func encode(_ value: Double) throws { data.encode(value) }
    mutating func encode(_ value: Float)  throws { data.encode(value) }
    mutating func encode(_ value: Int)    throws { data.encode(value) }
    mutating func encode(_ value: Int8)   throws { data.encode(value) }
    mutating func encode(_ value: Int16)  throws { data.encode(value) }
    mutating func encode(_ value: Int32)  throws { data.encode(value) }
    mutating func encode(_ value: Int64)  throws { data.encode(value) }
    mutating func encode(_ value: UInt)   throws { data.encode(value) }
    mutating func encode(_ value: UInt8)  throws { data.encode(value) }
    mutating func encode(_ value: UInt16) throws { data.encode(value) }
    mutating func encode(_ value: UInt32) throws { data.encode(value) }
    mutating func encode(_ value: UInt64) throws { data.encode(value) }
    mutating func encode<T: Encodable>(_ value: T) throws { try value.encode(to: DataFrameEncoding(data: data)) }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(DataFrameKeyedEncoding<NestedKey>(encoding: encoding))
    }
    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer { DataFrameUnkeyedEncoding(encoding: encoding) }
    mutating func superEncoder() -> Encoder { encoding }
}

fileprivate struct DataFrameSingleValueEncoding: SingleValueEncodingContainer {
    private let encoding: DataFrameEncoding
    var codingPath: [CodingKey] = []

    init(encoding: DataFrameEncoding) { self.encoding = encoding }
    
    var data: ColumnCollection { encoding.data }

    mutating func encodeNil()             throws { data.encode("nil") }
    mutating func encode(_ value: Bool)   throws { data.encode(value) }
    mutating func encode(_ value: String) throws { data.encode(value) }
    mutating func encode(_ value: Double) throws { data.encode(value) }
    mutating func encode(_ value: Float)  throws { data.encode(value) }
    mutating func encode(_ value: Int)    throws { data.encode(value) }
    mutating func encode(_ value: Int8)   throws { data.encode(value) }
    mutating func encode(_ value: Int16)  throws { data.encode(value) }
    mutating func encode(_ value: Int32)  throws { data.encode(value) }
    mutating func encode(_ value: Int64)  throws { data.encode(value) }
    mutating func encode(_ value: UInt)   throws { data.encode(value) }
    mutating func encode(_ value: UInt8)  throws { data.encode(value) }
    mutating func encode(_ value: UInt16) throws { data.encode(value) }
    mutating func encode(_ value: UInt32) throws { data.encode(value) }
    mutating func encode(_ value: UInt64) throws { data.encode(value) }
    mutating func encode<T: Encodable>(_ value: T) throws { try value.encode(to: DataFrameEncoding(data: data)) }
}
