//
//  CSVDecodingError.swift
//  SweepMap
//
//  Created by Doug on 12/10/24.
//  Copyright © 2024 Doug. All rights reserved.
//

public struct CSVDecodingError: Error, CustomStringConvertible {
    private let error: DecodingError

    public static func typeMismatch(_ type: any Any.Type, _ context: DecodingError.Context) -> CSVDecodingError {
        CSVDecodingError(error: DecodingError.typeMismatch(type, context))
    }

    public static func valueNotFound(_ type: any Any.Type, _ context: DecodingError.Context) -> CSVDecodingError {
        CSVDecodingError(error: DecodingError.valueNotFound(type, context))
    }

    public static func keyNotFound(_ type: any CodingKey, _ context: DecodingError.Context) -> CSVDecodingError {
        CSVDecodingError(error: DecodingError.keyNotFound(type, context))
    }

    public static func dataCorrupted(_ context: DecodingError.Context) -> CSVDecodingError {
        CSVDecodingError(error: DecodingError.dataCorrupted(context))
    }

    public static func dataCorruptedError<C>(forKey key: C.Key, in container: C, debugDescription: String) -> CSVDecodingError where C : KeyedDecodingContainerProtocol {
        CSVDecodingError(error: DecodingError.dataCorruptedError(forKey: key, in: container, debugDescription: debugDescription))
    }

    public static func dataCorruptedError(in container: any UnkeyedDecodingContainer, debugDescription: String) -> CSVDecodingError {
        CSVDecodingError(error: DecodingError.dataCorruptedError(in: container, debugDescription: debugDescription))
    }

    public static func dataCorruptedError(in container: any SingleValueDecodingContainer, debugDescription: String) -> CSVDecodingError {
        CSVDecodingError(error: DecodingError.dataCorruptedError(in: container, debugDescription: debugDescription))
    }
    
    public var description: String {
        switch error {
        case .typeMismatch(let anyType, let context):
            return """
                typeMismatch(
                    type: \"\(anyType)\",
                    codingPath: \"\(name(for: context.codingPath))\",
                    debugDescription: \(context.debugDescription.replacingOccurrences(of: "\"", with: "\""))
                )
                """
        case .valueNotFound(let anyType, let context):
            return """
                valueNotFound(
                    type: \"\(anyType)\",
                    codingPath: \"\(name(for: context.codingPath))\",
                    debugDescription: \(context.debugDescription.replacingOccurrences(of: "\"", with: "\""))
                )
                """
        case .keyNotFound(let anyKey, let context):
            return """
                keyNotFound(
                    key: \"\(anyKey)\",
                    codingPath: \"\(name(for: context.codingPath))\",
                    debugDescription: \(context.debugDescription.replacingOccurrences(of: "\"", with: "\""))
                )
                """
        case .dataCorrupted(let context):
            return """
                dataCorrupted(
                    codingPath: \"\(name(for: context.codingPath))\",
                    debugDescription: \(context.debugDescription.replacingOccurrences(of: "\"", with: "\""))
                )
                """
        default:
            return error.localizedDescription
        }
    }
    
    private func name(for codingPath: [CodingKey]) -> String {
        return codingPath.map(\.stringValue).joined(separator: ".")
    }
}
