//
//  DataDecodingError.swift
//  TabularCSV
//
//  Created by Doug on 12/10/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation

public struct DataDecodingError: Error, CustomStringConvertible, CustomDebugStringConvertible {
    public let error: DecodingError
    
    public var description: String {
        switch error {
        case .typeMismatch(_, let context):
            return cleanQuotes(context.debugDescription)
        case .valueNotFound(_, let context):
            return cleanQuotes(context.debugDescription)
        case .keyNotFound(_, let context):
            return cleanQuotes(context.debugDescription)
        case .dataCorrupted(let context):
            return cleanQuotes(context.debugDescription)
        default:
            return error.localizedDescription
        }
    }
    
    public var debugDescription: String {
        switch error {
        case .typeMismatch(let anyType, let context):
            return "typeMismatch(type: '\(anyType)', codingPath: '\(name(context.codingPath))', debugDescription: \(cleanQuotes(context.debugDescription)))"
        case .valueNotFound(let anyType, let context):
            return "valueNotFound(type: '\(anyType)', codingPath: '\(name(context.codingPath))', debugDescription: \(cleanQuotes(context.debugDescription)))"
        case .keyNotFound(let anyKey, let context):
            return "keyNotFound(key: '\(anyKey)', codingPath: '\(name(context.codingPath))', debugDescription: \(cleanQuotes(context.debugDescription)))"
        case .dataCorrupted(let context):
            return "dataCorrupted(codingPath: '\(name(context.codingPath))', debugDescription: \(cleanQuotes(context.debugDescription)))"
        default:
            return error.localizedDescription
        }
    }
    
    private func name(_ codingPath: [CodingKey]) -> String {
        return codingPath.map(\.stringValue).joined(separator: ".")
    }
    
    private func cleanQuotes(_ string: String) -> String {
        return string.replacingOccurrences(of: "'", with: "'")
    }

    public func withKey(_ key: CodingKey?, rowNumber: Int) -> DataDecodingError {
        guard let key else { return self }
        switch error {
        case .typeMismatch(let anyType, _):
            return DataDecodingError.typeMismatch(anyType, forKey: key, rowNumber: rowNumber)
        case .valueNotFound(let anyType, _):
            return DataDecodingError.valueNotFound(anyType, forKey: key, rowNumber: rowNumber)
        default:
            return self
        }
    }
    
    public static func typeMismatch(_ type: any Any.Type, _ context: DecodingError.Context) -> DataDecodingError {
        DataDecodingError(error: DecodingError.typeMismatch(type, context))
    }

    public static func valueNotFound(_ type: any Any.Type, _ context: DecodingError.Context) -> DataDecodingError {
        DataDecodingError(error: DecodingError.valueNotFound(type, context))
    }

    public static func keyNotFound(_ type: any CodingKey, _ context: DecodingError.Context) -> DataDecodingError {
        DataDecodingError(error: DecodingError.keyNotFound(type, context))
    }

    public static func dataCorrupted(_ context: DecodingError.Context) -> DataDecodingError {
        DataDecodingError(error: DecodingError.dataCorrupted(context))
    }

    public static func dataCorruptedError<C>(forKey key: C.Key, in container: C, debugDescription: String) -> DataDecodingError where C : KeyedDecodingContainerProtocol {
        DataDecodingError(error: DecodingError.dataCorruptedError(forKey: key, in: container, debugDescription: debugDescription))
    }

    public static func dataCorruptedError(in container: any UnkeyedDecodingContainer, debugDescription: String) -> DataDecodingError {
        DataDecodingError(error: DecodingError.dataCorruptedError(in: container, debugDescription: debugDescription))
    }

    public static func dataCorruptedError(in container: any SingleValueDecodingContainer, debugDescription: String) -> DataDecodingError {
        DataDecodingError(error: DecodingError.dataCorruptedError(in: container, debugDescription: debugDescription))
    }
    
    static func typeMismatch<T>(_ type: T.Type, forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.valueNotFound(T.self, context(description: "Value does not match expected type '\(type)'", forKey: key, rowNumber: rowNumber))
    }
    
    static func valueNotFound<T>(_ type: T.Type, forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.valueNotFound(T.self, context(description: "Value of type '\(type)' not available", forKey: key, rowNumber: rowNumber))
    }
    
    static func valueAlreadyDecoded<T>(_ type: T.Type, forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Value of type '\(type)' already decoded", forKey: key, rowNumber: rowNumber))
    }
    
    static func incorrectNilSequence<T>(_ type: T.Type, nilKey: CodingKey, currentKey: CodingKey?, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "decodeNil(forKey: '\(nilKey.stringValue)') and decodeNil(forKey: '\(currentKey?.stringValue ?? "nil")') are required to match for the sequence decodeNil(forKey) -> decodeNil(forKey), for type '\(type)'", forKey: nil, rowNumber: rowNumber))
    }
    
    static func incorrectSequence<T>(_ type: T.Type, nilKey: CodingKey, currentKey: CodingKey?, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "decodeNil(forKey: '\(nilKey.stringValue)') and decode(type, forKey: '\(currentKey?.stringValue ?? "nil")') are required to match for the sequence decodeNil(forKey) -> decode(type, forKey), for type '\(type)'", forKey: nil, rowNumber: rowNumber))
    }
    
    static func duplicateSequence<T>(_ type: T.Type, nilKey: CodingKey, currentKey: CodingKey?, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Too many calls to decode(type, forKey: '\(currentKey?.stringValue ?? "nil")'): decodeNil(forKey: '\(nilKey.stringValue)') and decode(type, forKey: '\(currentKey?.stringValue ?? "nil")') must be paired correctly for the sequence decodeNil(forKey) -> decode(type, forKey), for type '\(type)'", forKey: nil, rowNumber: rowNumber))
    }
    
    static func singleValueDecoding(rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Single value decoding error", rowNumber: rowNumber))
    }
    
    static func dataCorrupted(string: String, forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot decode '\(string)'", forKey: key, rowNumber: rowNumber))
    }
    
    static func isAtEnd(rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "No more rows available", rowNumber: rowNumber))
    }
    
    static func nestedContainer(forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot create nested container", forKey: key, rowNumber: rowNumber))
    }
    
    static func unkeyedContainer(rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot create unkeyed container", rowNumber: rowNumber))
    }
    
    private static func context(description: String, forKey key: CodingKey? = nil, rowNumber: Int) -> DecodingError.Context {
        var codingPath = [CodingKey]()
        var description = description
        if let key = key {
            codingPath.append(key)
            description += " for key '\(key.stringValue)'"
        }
        description += "\(atRow(rowNumber))."
        return DecodingError.Context(codingPath: codingPath, debugDescription: description)
    }
    
    private static func atRow(_ rowNumber: Int) -> String {
        rowNumber >= 0 ? " at row \(rowNumber)" : ""
    }
}
