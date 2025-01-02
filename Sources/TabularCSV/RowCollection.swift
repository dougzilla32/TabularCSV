//
//  RowCollection.swift
//  SweepMap
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public protocol TypedRow {
    subscript<T: LosslessStringConvertible>(position: Int, type: T.Type) -> T? { get }
    var count: Int { get }
}

public protocol TypedRows: Collection where Element: TypedRow { }

extension DataFrame.Row: TypedRow { }

extension DataFrame.Rows: TypedRows { }

extension Array: TypedRow where Element == String? {
    public subscript<T: LosslessStringConvertible>(position: Int, type: T.Type) -> T? {
        guard let string = self[position] else { return nil }
        return T(string)
    }
}

extension Array: TypedRows where Element == [String?] { }

final class RowCollection<Rows: TypedRows> {
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
    
    func nextValue<T: LosslessStringConvertible & CSVPrimitiveType>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = nextValueIfPresent(T.self) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    func nextValueIfPresent<T: LosslessStringConvertible & CSVPrimitiveType>(_ type: T.Type, isPeek: Bool = false) -> T? {
        guard let row = currentRow,
            currentColumnIndex < row.count
        else {
            return nil
        }
        var value = row[rowMapping?[currentColumnIndex] ?? currentColumnIndex, type]
        if type == String.self, value == nil {
            value = "" as? T
        }
        if !isPeek {
            csvTypes?.append(type.csvType)
            currentColumnIndex += 1
        }
        return value
    }
    
    func nextString(forKey key: CodingKey? = nil) throws -> String {
        try nextValue(String.self, forKey: key)
    }
    
    func nextStringIfPresent() -> String? {
        nextValueIfPresent(String.self)
    }
    
    func peekStringIfPresent() -> String? {
        nextValueIfPresent(String.self, isPeek: true)
    }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        if let parser = options.parserForType(type) {
            let string = try nextString(forKey: key)
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
            guard let string = nextStringIfPresent(), !string.isEmpty else { return nil }
            return try parse(type, string: string, parser: parser)
        } else {
            guard let string = peekStringIfPresent(), !string.isEmpty else { return nil }
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
