//
//  RowCollection.swift
//  TabularCSV
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public protocol DataRow {
    subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? { get }
    var count: Int { get }
}

public protocol DataRows: Collection where Element: DataRow { }

extension DataFrame.Row: DataRow {
    public subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? {
        self[position, type]
    }
}

extension DataFrame.Rows: DataRows { }

extension Array: DataRow where Element == String? {
    public subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? {
        guard let string = self[position] else { return nil }
        if options.nilEncodings.contains(string) { return nil }
        if type == Bool.self {
            if options.trueEncodings.contains(string) { return true as? T }
            if options.falseEncodings.contains(string) { return false as? T }
        }
        return T(string)
    }
}

extension Array: DataRows where Element == [String?] { }

final class RowCollection<Rows: DataRows> {
    let rowCount: Int
    private var rowsIterator: Rows.Iterator
    private let rowMapping: [Int?]?
    private let options: ReadingOptions
    private var currentRow: Rows.Element?
    private(set) var rowNumber: Int
    private(set) var currentRowIndex: Int
    private var currentColumnIndex: Int
    private(set) var csvTypes: [CSVType]?

    init(rows: Rows, rowMapping: [Int?]?, withTypes: Bool, options: ReadingOptions) {
        self.rowCount = rows.count
        self.rowsIterator = rows.makeIterator()
        self.rowMapping = rowMapping
        self.options = options
        self.currentRow = nil
        self.rowNumber = (options.csvReadingOptions.hasHeaderRow && !withTypes) ? 1 : 0
        self.currentRowIndex = 0
        self.currentColumnIndex = -1
        if withTypes {
            csvTypes = []
        }
    }
    
    func nextRow() throws {
        guard nextRowIfPresent() else {
            throw CSVDecodingError.isAtEnd(rowNumber: rowNumber)
        }
    }
    
    func nextRowIfPresent() -> Bool {
        guard let row = rowsIterator.next() else { return false }
        
        currentRow = row
        rowNumber += 1
        currentRowIndex += 1
        currentColumnIndex = 0
        return true
    }
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = decodeNextIfPresent(T.self) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, isPeek: Bool = false) -> T? {
        guard let row = currentRow,
            currentColumnIndex < row.count
        else {
            return nil
        }
        var value = row[rowMapping?[currentColumnIndex] ?? currentColumnIndex, type, options]
        if options.nilAsEmptyString, type == String.self, value == nil {
            value = "" as? T
        }
        if !isPeek {
            csvTypes?.append(type.csvType)
            currentColumnIndex += 1
        }
        return value
    }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        if let parser = options.parserForType(type) {
            let string = try decodeNext(String.self, forKey: key)
            return try parse(type, string: string, forKey: key, parser: parser)
        } else {
            do {
                return try T(from: decoder)
            } catch CodableStringError.invalidFormat(let string) {
                throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
            } catch let csvError as CSVDecodingError {
                switch csvError.error {
                case .dataCorrupted(let context):
                    var codingPath: [CodingKey] = []
                    if let key = key { codingPath.append(key) }
                    throw CSVDecodingError.dataCorrupted(
                        DecodingError.Context(
                            codingPath: codingPath,
                            debugDescription: context.debugDescription))
                default:
                    throw csvError
                }
            }
        }
    }
    
    func decodeIfPresent<T: Decodable>(_ type: T.Type, decoder: Decoder) throws -> T? {
        if let parser = options.parserForType(type) {
            guard let string = decodeNextIfPresent(String.self), !string.isEmpty else { return nil }
            return try parse(type, string: string, parser: parser)
        } else {
            guard let string = decodeNextIfPresent(String.self, isPeek: true), !string.isEmpty else {
                _ = decodeNextIfPresent(String.self)
                return nil
            }
            do {
                return try T(from: decoder)
            } catch CodableStringError.invalidFormat(let string) {
                throw CSVDecodingError.dataCorrupted(string: string, rowNumber: rowNumber)
            }
        }
    }
    
    private func parse<T>(_ type: T.Type, string: String, forKey key: CodingKey? = nil, parser: ((String) -> Any)) throws -> T {
        guard let value = parser(string) as? T else {
            throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    struct ValueIndex: Equatable {
        let row: Int
        let column: Int
    }
    
    func getValueIndex() -> ValueIndex {
        return ValueIndex(row: currentRowIndex, column: currentColumnIndex)
    }
    
    func checkValueIndex(_ index: ValueIndex) throws {
        guard index == getValueIndex() else {
            throw CSVDecodingError.dataCorrupted(string: "Value was already decoded", rowNumber: rowNumber)
        }
    }

    func checkValueIndexIfPresent(_ index: ValueIndex) -> Bool{
        index == getValueIndex()
    }
}
