//
//  StringEncoder.swift
//  SweepMap
//
//  From: https://stackoverflow.com/questions/45169254/custom-swift-encoder-decoder-for-the-strings-resource-format
//

/// An object that encodes instances of a data type
/// as strings following the simple strings file format.
public struct StringEncoder {
    private let options: WritingOptions

    public init(options: WritingOptions) {
        self.options = options
    }

    /// Returns a strings file-encoded representation of the specified value.
    public func encode<T: Encodable>(_ value: T) throws -> [String] {
        let stringEncoding = StringEncoding()
        try value.encode(to: stringEncoding)
        return stringEncoding.data.strings
    }
}

fileprivate struct StringEncoding: Encoder {
    fileprivate final class Data {
        private(set) var strings: [String] = []
        
        func encode(_ value: String) { strings.append(value) }
    }
    
    fileprivate var data: Data
    var codingPath: [CodingKey] = []
    let userInfo: [CodingUserInfoKey : Any] = [:]
    
    init(to encodedData: Data = Data()) {
        self.data = encodedData
    }
    
    func container<Key: CodingKey>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> {
        KeyedEncodingContainer(StringKeyedEncoding<Key>(to: data))
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        StringUnkeyedEncoding(to: data)
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        StringSingleValueEncoding(to: data)
    }
}

fileprivate struct StringKeyedEncoding<Key: CodingKey>: KeyedEncodingContainerProtocol {
    private let data: StringEncoding.Data
    var codingPath: [CodingKey] = []
    
    init(to data: StringEncoding.Data) { self.data = data }
    
    mutating func encodeNil(                forKey key: Key) throws { data.encode("nil") }
    mutating func encode(_ value: Bool,     forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: String,   forKey key: Key) throws { data.encode(value) }
    mutating func encode(_ value: Double,   forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: Float,    forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: Int,      forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: Int8,     forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: Int16,    forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: Int32,    forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: Int64,    forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt,     forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt8,    forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt16,   forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt32,   forKey key: Key) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt64,   forKey key: Key) throws { data.encode(value.description) }
    mutating func encode<T: Encodable>(_ value: T, forKey key: Key) throws { try value.encode(to: StringEncoding(to: data)) }
    
    mutating func encodeIfPresent(_ value: Bool?,    forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: String?,  forKey key: Key) throws { data.encode(value ?? "") }
    mutating func encodeIfPresent(_ value: Double?,  forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: Float?,   forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: Int?,     forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: Int8?,    forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: Int16?,   forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: Int32?,   forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: Int64?,   forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: UInt?,    forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: UInt8?,   forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: UInt16?,  forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: UInt32?,  forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent(_ value: UInt64?,  forKey key: Key) throws { data.encode(value?.description ?? "") }
    mutating func encodeIfPresent<T: Encodable>(_ value: T?, forKey key: Key) throws { try value.encode(to: StringEncoding(to: data)) }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type,
        forKey key: Key) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(StringKeyedEncoding<NestedKey>(to: data))
    }
    
    mutating func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        StringUnkeyedEncoding(to: data)
    }
    
    mutating func superEncoder() -> Encoder {
        superEncoder(forKey: Key(stringValue: "super")!)
    }
    
    mutating func superEncoder(forKey key: Key) -> Encoder {
        StringEncoding(to: data)
    }
}

fileprivate struct StringUnkeyedEncoding: UnkeyedEncodingContainer {
    private let data: StringEncoding.Data
    var codingPath: [CodingKey] = []
    var count: Int { data.strings.count }

    init(to data: StringEncoding.Data) { self.data = data }
    
    mutating func encodeNil()             throws { data.encode("nil") }
    mutating func encode(_ value: Bool)   throws { data.encode(value.description) }
    mutating func encode(_ value: String) throws { data.encode(value) }
    mutating func encode(_ value: Double) throws { data.encode(value.description) }
    mutating func encode(_ value: Float)  throws { data.encode(value.description) }
    mutating func encode(_ value: Int)    throws { data.encode(value.description) }
    mutating func encode(_ value: Int8)   throws { data.encode(value.description) }
    mutating func encode(_ value: Int16)  throws { data.encode(value.description) }
    mutating func encode(_ value: Int32)  throws { data.encode(value.description) }
    mutating func encode(_ value: Int64)  throws { data.encode(value.description) }
    mutating func encode(_ value: UInt)   throws { data.encode(value.description) }
    mutating func encode(_ value: UInt8)  throws { data.encode(value.description) }
    mutating func encode(_ value: UInt16) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt32) throws { data.encode(value.description) }
    mutating func encode(_ value: UInt64) throws { data.encode(value.description) }
    mutating func encode<T: Encodable>(_ value: T) throws { try value.encode(to: StringEncoding(to: data)) }
    
    mutating func nestedContainer<NestedKey: CodingKey>(
        keyedBy keyType: NestedKey.Type) -> KeyedEncodingContainer<NestedKey>
    {
        KeyedEncodingContainer(StringKeyedEncoding<NestedKey>(to: data))
    }
    
    mutating func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        StringUnkeyedEncoding(to: data)
    }
    
    mutating func superEncoder() -> Encoder {
        StringEncoding(to: data)
    }
}

fileprivate struct StringSingleValueEncoding: SingleValueEncodingContainer {
    private let data: StringEncoding.Data
    var codingPath: [CodingKey] = []

    init(to data: StringEncoding.Data) { self.data = data }
    
    mutating func encodeNil()             throws { data.encode("nil")               }
    mutating func encode(_ value: Bool)   throws { data.encode(value.description)   }
    mutating func encode(_ value: String) throws { data.encode(value)               }
    mutating func encode(_ value: Double) throws { data.encode(value.description)   }
    mutating func encode(_ value: Float)  throws { data.encode(value.description)   }
    mutating func encode(_ value: Int)    throws { data.encode(value.description)   }
    mutating func encode(_ value: Int8)   throws { data.encode(value.description)   }
    mutating func encode(_ value: Int16)  throws { data.encode(value.description)   }
    mutating func encode(_ value: Int32)  throws { data.encode(value.description)   }
    mutating func encode(_ value: Int64)  throws { data.encode(value.description)   }
    mutating func encode(_ value: UInt)   throws { data.encode(value.description)   }
    mutating func encode(_ value: UInt8)  throws { data.encode(value.description)   }
    mutating func encode(_ value: UInt16) throws { data.encode(value.description)   }
    mutating func encode(_ value: UInt32) throws { data.encode(value.description)   }
    mutating func encode(_ value: UInt64) throws { data.encode(value.description)   }
    mutating func encode<T: Encodable>(_ value: T) throws { try value.encode(to: StringEncoding(to: data)) }
}
