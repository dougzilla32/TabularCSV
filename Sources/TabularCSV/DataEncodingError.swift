//
//  DataEncodingError.swift
//  TabularCSV
//
//  Created by Doug on 1/22/25.
//

public struct DataEncodingError: Error, CustomStringConvertible, CustomDebugStringConvertible {
    public let error: EncodingError
    
    public var description: String {
        switch error {
        case .invalidValue(_, let context):
            return context.debugDescription
        default:
            return error.localizedDescription
        }
    }
    
    public var debugDescription: String {
        switch error {
        case .invalidValue(let anyType, let context):
            return "invalidValue(type: '\(anyType)', codingPath: '\(name(context.codingPath))', debugDescription: \(context.debugDescription))"
        default:
            return error.localizedDescription
        }
    }
    
    private func name(_ codingPath: [CodingKey]) -> String {
        return codingPath.map(\.stringValue).joined(separator: ".")
    }
    
    static func mismatchedHeader(headerCount: Int, dataCount: Int) -> DataEncodingError {
        DataEncodingError(error: EncodingError.invalidValue(headerCount, context(description: "The number of columns in the header '\(headerCount)' does not match the number of columns in the data '\(dataCount)'", rowNumber: -1)))
    }
    
    static func invalidUnkeyedValue<T>(_ value: T) -> DataEncodingError {
        DataEncodingError(error: EncodingError.invalidValue(value, context(description: "The value '\(value)' cannot be encoded without a key.", rowNumber: -1)))
    }
    
    private static func context(description: String, forKey key: CodingKey? = nil, rowNumber: Int) -> EncodingError.Context {
        var codingPath = [CodingKey]()
        var description = description
        if let key = key {
            codingPath.append(key)
            description += " for key '\(key.stringValue)'"
        }
        description += "\(atRow(rowNumber))."
        return EncodingError.Context(codingPath: codingPath, debugDescription: description)
    }
    
    private static func atRow(_ rowNumber: Int) -> String {
        rowNumber >= 0 ? " at row \(rowNumber)" : ""
    }
}
