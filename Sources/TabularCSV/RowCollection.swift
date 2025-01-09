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

struct HeaderAndType {
    let name: String
    let type: CSVType
}

public enum RowTransform {
    case none
    case permutation([Int?])
    case map([String: Int]?)
    
    init(_ permutation: [Int?]?) {
        if let permutation {
            self = .permutation(permutation)
        } else {
            self = .none
        }
    }
    
    init(_ keys: [String]?) {
        if let keys {
            let mapping = Dictionary(uniqueKeysWithValues: keys.enumerated().map { ($1, $0) })
            self = .map(mapping)
        } else {
            self = .map(nil)
        }
    }
}

final class RowCollection<Rows: DataRows> {
    let rowCount: Int
    private var rowsIterator: Rows.Iterator
    private let transform: RowTransform
    private let options: ReadingOptions
    private var currentRow: Rows.Element?
    private(set) var rowNumber: Int
    private(set) var currentRowIndex: Int
    private var currentColumnIndex: Int
    private(set) var headerAndTypes: [HeaderAndType]?
    
    init(rows: Rows, transform: RowTransform, options: ReadingOptions) {
        self.rowCount = rows.count
        self.rowsIterator = rows.makeIterator()
        self.transform = transform
        self.options = options
        self.currentRow = nil
        self.rowNumber = options.csvReadingOptions.hasHeaderRow ? 1 : 0
        self.currentRowIndex = 0
        self.currentColumnIndex = -1

        if case .map = transform {
            headerAndTypes = []
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
    
    func decodeNext<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil, isPeek: Bool = false) throws -> T {
        guard let value = decodeNextIfPresent(T.self, forKey: key, isPeek: isPeek) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    func decodeNextIfPresent<T: CSVPrimitive>(_ type: T.Type, forKey key: CodingKey? = nil, isPeek: Bool = false) -> T? {
        guard let row = currentRow,
              currentColumnIndex < row.count
        else {
            return nil
        }
        
        let index: Int?
        switch transform {
        case .none:
            index = currentColumnIndex
        case .permutation(let permutation):
            index = permutation[currentColumnIndex]
        case .map(let map):
            if let map, let key {
                index = map[key.stringValue]
            } else {
                index = currentColumnIndex
            }
        }

        var value: T?
        if let index {
            value = row[index, type, options]
        } else {
            value = nil
        }

        if !isPeek {
            if options.nilAsEmptyString, type == String.self, value == nil {
                value = "" as? T
            }
            if let key {
                headerAndTypes?.append(.init(name: key.stringValue, type: type.csvType))
            }
            currentColumnIndex += 1
        }
        return value
    }
    
    func decodeNext<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T {
        if let parser = options.parserForType(type) {
            let string = try decodeNext(String.self, forKey: key)
            return try parse(type, string: string, forKey: key, parser: parser)
        } else {
            do {
                // FIXME: do this another way -- next value may not be a string, this could be an entire row
                // _ = try decodeNext(String.self, forKey: key, isPeek: true)
                if let key {
                    switch transform {
                    case .map(_):
                        let string = try decodeNext(String.self, forKey: key)
                        return try T(from: try StringRowsDecoder.singleton(rows: [[string]], options: options))
                    default:
                        headerAndTypes?.append(.init(name: key.stringValue, type: .string))
                        return try T(from: decoder)
                    }
                } else {
                    return try T(from: decoder)
                }
            } catch CodableStringError.invalidFormat(let string) {
                throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
            } catch let csvError as CSVDecodingError {
                switch csvError.error {
                case .dataCorrupted(let context):
                    var codingPath: [CodingKey] = []
                    if let key { codingPath.append(key) }
                    codingPath += csvError.codingPath
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
    
    func decodeNextIfPresent<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoder: Decoder) throws -> T? {
        if let parser = options.parserForType(type) {
            guard let string = decodeNextIfPresent(String.self, forKey: key), !string.isEmpty else { return nil }
            return try parse(type, string: string, parser: parser)
        } else {
            guard decodeNextIfPresent(String.self, forKey: key, isPeek: true) != nil else {
                _ = decodeNextIfPresent(String.self, forKey: key)
                return nil
            }
            if let key {
                headerAndTypes?.append(.init(name: key.stringValue, type: .string))
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
