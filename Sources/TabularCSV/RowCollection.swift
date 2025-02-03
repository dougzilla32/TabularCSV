//
//  RowCollection.swift
//  TabularCSV
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

// MARK: Data rows protocol

public protocol DataRows: Collection where Element: DataRow { }

public protocol DataRow {
    var count: Int { get }
    func string(at index: Int, options: ReadingOptions) -> String?
}

extension DataRow {
    public func decodePrim<T: LosslessStringConvertible>(at index: Int, type: T.Type, forKey key: CodingKey? = nil, rowNumber: Int, options: ReadingOptions) throws -> T {
        guard let string = string(at: index, options: options) else {
            throw DataDecodingError.valueNotFound(type, forKey: key, rowNumber: rowNumber)
        }
        return try decodePrim(string: string, type: type, forKey: key, rowNumber: rowNumber, options: options)
    }
    
    public func decodePrimIfPresent<T: LosslessStringConvertible>(at index: Int, type: T.Type, forKey key: CodingKey? = nil, rowNumber: Int, options: ReadingOptions) throws -> T? {
        guard let string = string(at: index, options: options) else {
            return nil
        }
        return try decodePrim(string: string, type: type, forKey: key, rowNumber: rowNumber, options: options)
    }
    
    public func decodePrim<T: LosslessStringConvertible>(string: String, type: T.Type, forKey key: CodingKey?, rowNumber: Int, options: ReadingOptions) throws -> T {
        if type == Bool.self {
            if options.trueEncodings.contains(string) { return true as! T }
            if options.falseEncodings.contains(string) { return false as! T }
        }
        guard let value = T(string) else {
            throw DataDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
}

extension DataRow {
    public func decodeString(at index: Int, forKey key: CodingKey? = nil, rowNumber: Int, options: ReadingOptions) throws -> String {
        var string = string(at: index, options: options)
        if string == nil, options.nilAsEmptyString {
            string = ""
        }
        guard let string else {
            throw DataDecodingError.valueNotFound(String.self, forKey: key, rowNumber: rowNumber)
        }
        return string
    }
    
    public func decodeStringIfPresent(at index: Int, forKey key: CodingKey? = nil, rowNumber: Int, options: ReadingOptions) throws -> String? {
        var string = string(at: index, options: options)
        if string == nil, options.nilAsEmptyString {
            string = ""
        }
        guard let string else {
            return nil
        }
        return string
    }
}

// MARK: DataFrame rows

extension DataFrame.Rows: DataRows { }

extension DataFrame.Row: DataRow {
    public func string(at index: Int, options: ReadingOptions) -> String? {
        self[index, String.self]
    }
}

// MARK: String rows

extension Array: DataRows where Element == [String?] { }

extension Array: DataRow where Element == String? {
    public func string(at index: Int, options: ReadingOptions) -> String? {
        guard let value = self[index], !options.nilEncodings.contains(value) else {
            return nil
        }
        return value
    }
}

// MARK: CSVField

struct CSVField: Hashable {
    let name: String
    let type: CSVType
}
