//
//  CodableString.swift
//  SweepMap
//
//  Created by Doug on 11/30/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation

// MARK: CodableString

// This protocol defines a generic interface for a property wrapper that facilitates
// encoding and decoding between a String representation and a specific value type.
// Optional value types must be handled with both a non-optional CodableString property
// wrapper along with a corresponding OptionalCodableString property wrapper.
//
// Requirements:
// - The property wrapper must provide an initializer with the underlying wrapped value. This
//   initializer can be automatically generated by the struct.
// - It must be able to decode a String into that value type (static decode method).
// - It must be able to encode a value type back to a String (static encode method).
public protocol CodableString: Codable {
    // Specify the type for the wrapped value
    associatedtype ValueType
    
    // The wrapped value
    var wrappedValue: ValueType { get }
    
    // Initializes the wrapper with a given underlying value.
    init(wrappedValue: ValueType)
    
    // A static method to convert a given string into the specified ValueType.
    // Returns an optional, since the string may fail to decode properly.
    static func decode(string: String) -> ValueType?
    
    // A static method to convert a given ValueType into its string representation.
    static func encode(_ value: ValueType) -> String
}

public extension CodableString {
    // Decodable conformance:
    // This initializer is called when decoding the property from a decoder.
    // It attempts to decode a single string value, then uses the `decode(string:)`
    // method to transform that string into the underlying ValueType.
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let string = try container.decode(String.self)
        
        guard let value = Self.decode(string: string) else {
            throw CodableStringError.invalidFormat(string: string)
        }
        
        self.init(wrappedValue: value)
    }

    // Encodable conformance:
    // This method is called when encoding the property to an encoder.
    // It uses the `encode(_:)` method to convert the underlying value to a string,
    // and then encodes that string into the container.
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(Self.encode(wrappedValue))
    }
}

// This protocol extends Codable to handle optional wrapped values for types that conform to CodableString.
// It allows properties to be optional while still leveraging the encoding and decoding mechanisms
// defined in CodableString.
//
// Requirements:
// - Conforms to `Codable` and `OptionalCodingWrapper` (a protocol which handles optional coding logic).
// - Manages an optional wrapped value of the type specified by `CodableStringType.ValueType`.
public protocol OptionalCodableString: Codable, OptionalCodingWrapper {
    // Specify the type for the non-optional CodableString property wrapper.
    associatedtype CodableStringType: CodableString
    
    // The optional wrapped value.
    var wrappedValue: CodableStringType.ValueType? { get }
    
    // Initializes the wrapper with an optional underlying value.
    init(wrappedValue: CodableStringType.ValueType?)
}

public extension OptionalCodableString {
    // Decodable conformance:
    // Custom initializer to decode the property from a decoder.
    // This handles the decoding of optional string values into their corresponding ValueType.
    // - Throws: `CodableStringError.invalidFormat` if the string cannot be decoded.
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let string = try container.decode(String.self)
        let value = try Self.decode(string: string)
        self.init(wrappedValue: value)
    }

    // Encodable conformance:
    // Encodes the property to an encoder.
    // Converts the optional wrapped value into a string before encoding.
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(Self.encode(wrappedValue))
    }
    
    /// Decodes a string into an optional ValueType.
    ///
    /// - Parameter string: The string to decode.
    /// - Returns: The decoded ValueType if successful, or `nil` if the string is empty.
    /// - Throws: `CodableStringError.invalidFormat` if the string cannot be decoded and is not empty.
    static func decode(string: String) throws -> CodableStringType.ValueType? {
        if let value = CodableStringType.decode(string: string) {
            return value
        } else if string.isEmpty {
            return nil
        } else {
            throw CodableStringError.invalidFormat(string: string)
        }
    }
    
    /// Encodes an optional ValueType into its string representation.
    ///
    /// - Parameter value: The optional ValueType to encode.
    /// - Returns: A string representation of the ValueType, or an empty string if `nil`.
    static func encode(_ value: CodableStringType.ValueType?) -> String {
        guard let value = value else {
            return ""
        }
        return CodableStringType.encode(value)
    }
}

public enum CodableStringError: Error {
    // Indicates that the string format is invalid and cannot be decoded.
    case invalidFormat(string: String)
}

//  MARK: OptionalCodingWrapper is from:
//    https://github.com/GottaGetSwifty/CodableWrappers/blob/1b449bf3f19d3654571f00a7726786683dc950f0/Sources/CodableWrappers/OptionalWrappers.swift#L34
//  It is referenced here:
//    https://forums.swift.org/t/using-property-wrappers-with-codable/29804/12
//

/// Protocol for a PropertyWrapper to properly handle Coding when the wrappedValue is Optional
public protocol OptionalCodingWrapper {
    associatedtype WrappedType: ExpressibleByNilLiteral
    var wrappedValue: WrappedType { get }
    init(wrappedValue: WrappedType)
}

public extension KeyedDecodingContainer {
    // This is used to override the default decoding behavior for OptionalCodingWrapper to allow a value to avoid a missing key Error
    func decode<T>(_ type: T.Type, forKey key: KeyedDecodingContainer<K>.Key) throws -> T where T : Decodable, T: OptionalCodingWrapper {
        return try decodeIfPresent(T.self, forKey: key) ?? T(wrappedValue: nil)
    }
}

public extension KeyedEncodingContainer {
    // Used to make make sure OptionalCodingWrappers encode no value when it's wrappedValue is nil.
    mutating func encode<T>(_ value: T, forKey key: KeyedEncodingContainer<K>.Key) throws where T: Encodable, T: OptionalCodingWrapper {
        // Currently uses Mirror...this should really be avoided, but I'm not sure there's another way to do it cleanly.
//        let mirror = Mirror(reflecting: value.wrappedValue)
//        guard mirror.displayStyle != .optional || !mirror.children.isEmpty else {
//            return
//        }

        try encodeIfPresent(value, forKey: key)
    }
}

// MARK: - Immutable OrderedSet used by boolean coders

public struct OrderedSet<T: Hashable & Sendable>: ExpressibleByArrayLiteral, Sendable {
    private let array: [T]
    private let set: Set<T>

    public init(arrayLiteral elements: T...) {
        array = elements
        set = Set(elements)
    }

    public var count: Int { array.count }

    public var elements: [T] { array }

    public func contains(_ element: T) -> Bool { set.contains(element) }

    public subscript(index: Int) -> T { array[index] }
}

// MARK: - Coders for booleans

public protocol CodableBool: CodableString & Hashable where ValueType == Bool {
    static var trueValues: OrderedSet<String> { get }
    static var falseValues: OrderedSet<String> { get }
}

public extension CodableBool {
    static func decode(string: String) -> Bool? { trueValues.contains(string) ? true : (falseValues.contains(string) ? false : nil) }
    static func encode(_ value: Bool) -> String { value ? trueValues[0] : falseValues[0] }
}

@propertyWrapper
public struct BoolCoder: CodableBool {
    public var wrappedValue: Bool
    public init(wrappedValue: Bool) { self.wrappedValue = wrappedValue }
    public static let trueValues: OrderedSet<String> = ["true", "True", "TRUE",  "1"]
    public static let falseValues: OrderedSet<String> = ["false", "False", "FALSE", "0"]
}

@propertyWrapper
public struct OneZero: CodableBool {
    public var wrappedValue: Bool
    public init(wrappedValue: Bool) { self.wrappedValue = wrappedValue }
    public static let trueValues: OrderedSet<String> = ["1"]
    public static let falseValues: OrderedSet<String> = ["0"]
}

@propertyWrapper
public struct OneZeroOptional: OptionalCodableString {
    public typealias CodableStringType = OneZero
    public var wrappedValue: Bool?
    public init(wrappedValue: Bool?) { self.wrappedValue = wrappedValue }
}

@propertyWrapper
public struct OnOff: CodableBool {
    public let wrappedValue: Bool
    public init(wrappedValue: Bool) { self.wrappedValue = wrappedValue }
    public static let trueValues: OrderedSet<String> = ["on", "On", "ON"]
    public static let falseValues: OrderedSet<String> = ["off", "Off", "OFF"]
}

@propertyWrapper
public struct OnOffOptional: OptionalCodableString {
    public typealias CodableStringType = OnOff
    public var wrappedValue: Bool?
    public init(wrappedValue: Bool?) { self.wrappedValue = wrappedValue }
}

@propertyWrapper
public struct YesNo: CodableBool {
    public let wrappedValue: Bool
    public init(wrappedValue: Bool) { self.wrappedValue = wrappedValue }
    public static let trueValues: OrderedSet<String> = ["yes", "Yes", "YES"]
    public static let falseValues: OrderedSet<String> = ["no", "No", "NO"]
}

@propertyWrapper
public struct YesNoOptional: OptionalCodableString {
    public typealias CodableStringType = YesNo
    public var wrappedValue: Bool?
    public init(wrappedValue: Bool?) { self.wrappedValue = wrappedValue }
}

// MARK: - Coders for enum types where `rawValue` is a String

// A property wrapper for encoding and decoding a non-optional enum whose `rawValue` is a `String`.
@propertyWrapper
public struct EnumCodable<T: RawRepresentable>: CodableString where T.RawValue == String {
    public typealias ValueType = T
    public var wrappedValue: T
    public init(wrappedValue: T) { self.wrappedValue = wrappedValue }

    public static func decode(string: String) -> T? { T(rawValue: string) }
    public static func encode(_ value: T) -> String { value.rawValue }
}

// A property wrapper for encoding and decoding an optional enum whose `rawValue` is a `String`.
@propertyWrapper
public struct EnumOptionalCodable<T: RawRepresentable>: OptionalCodableString where T.RawValue == String {
    public typealias CodableStringType = EnumCodable<T>
    public var wrappedValue: T?
    public init(wrappedValue: T?) { self.wrappedValue = wrappedValue }
}

// MARK: Coders for Date

@propertyWrapper
public struct DateAndTimeCodable: CodableString, Hashable {
    public let wrappedValue: Date
    public init(wrappedValue: Date) { self.wrappedValue = wrappedValue }

    private static let formatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy/MM/dd hh:mm:ss a"
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter
    }()

    public static func decode(string: String) -> Date? { DateAndTimeCodable.formatter.date(from: string) }
    public static func encode(_ value: Date) -> String { DateAndTimeCodable.formatter.string(from: value) }
}

@propertyWrapper
public struct DateCodable: CodableString, Hashable {
    public typealias ValueType = Date
    public let wrappedValue: Date
    public init(wrappedValue: Date) { self.wrappedValue = wrappedValue }
    
    private static let formatter: DateFormatter = {
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy/MM/dd"
        formatter.locale = Locale(identifier: "en_US_POSIX")
        return formatter
    }()

    public static func decode(string: String) -> Date? { DateCodable.formatter.date(from: string) }
    public static func encode(_ value: Date) -> String { DateCodable.formatter.string(from: value) }
}
