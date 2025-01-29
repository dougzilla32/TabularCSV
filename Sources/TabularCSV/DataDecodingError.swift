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
            return context.debugDescription
        case .valueNotFound(_, let context):
            return context.debugDescription
        case .keyNotFound(_, let context):
            return context.debugDescription
        case .dataCorrupted(let context):
            return context.debugDescription
        default:
            return error.localizedDescription
        }
    }
    
    public var debugDescription: String {
        switch error {
        case .typeMismatch(let anyType, let context):
            return "typeMismatch(type: '\(anyType)', codingPath: '\(name(context.codingPath))', debugDescription: \(context.debugDescription))"
        case .valueNotFound(let anyType, let context):
            return "valueNotFound(type: '\(anyType)', codingPath: '\(name(context.codingPath))', debugDescription: \(context.debugDescription))"
        case .keyNotFound(let anyKey, let context):
            return "keyNotFound(key: '\(anyKey)', codingPath: '\(name(context.codingPath))', debugDescription: \(context.debugDescription))"
        case .dataCorrupted(let context):
            return "dataCorrupted(codingPath: '\(name(context.codingPath))', debugDescription: \(context.debugDescription))"
        default:
            return error.localizedDescription
        }
    }
    
    private func name(_ codingPath: [CodingKey]) -> String {
        return codingPath.map(\.stringValue).joined(separator: ".")
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
    
    static func typeMismatch<T>(_ type: T.Type, forKey key: CodingKey, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.valueNotFound(T.self, context(description: "Value does not match expected type '\(type)'", forKey: key, rowNumber: rowNumber))
    }
    
    static func valueNotFound<T>(_ type: T.Type, forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.valueNotFound(T.self, context(description: "Value of type '\(type)' not available", forKey: key, rowNumber: rowNumber))
    }
    
    static func headerIsNeeded(error: Error) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Header must be specified when using an irregular decodable type: \(error)", rowNumber: -1))
    }
    
    static func decoder(_ message: String, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: message, forKey: nil, rowNumber: rowNumber))
    }
    
    static func dataCorrupted(string: String, forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot decode '\(string)'", forKey: key, rowNumber: rowNumber))
    }
    
    static func nestedContainer(forKey key: CodingKey? = nil, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot create nested container", forKey: key, rowNumber: rowNumber))
    }
    
    static func unkeyedContainer(rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot create unkeyed container", rowNumber: rowNumber))
    }
    
    static func superDecoder(forKey key: CodingKey, rowNumber: Int) -> DataDecodingError {
        DataDecodingError.dataCorrupted(context(description: "Cannot create super decoder", forKey: key, rowNumber: rowNumber))
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
