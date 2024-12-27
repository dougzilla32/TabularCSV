//
//  RowCollection.swift
//  SweepMap
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

protocol TypedRow: Collection {
    subscript<T>(position: Int, type: T.Type) -> T? { get }
    
    var count: Int { get }
}

extension DataFrame.Row: TypedRow { }

extension Array: TypedRow where Element == String? {
    subscript<T>(position: Int, type: T.Type) -> T? {
        self[position] as? T
    }
}

protocol DataDecoder: Decoder {
    func nextString(forKey key: CodingKey?) throws -> String

    func nextStringIfPresent() -> String?
    
    func peekStringIfPresent() -> String?
}

final class RowCollection<Row: TypedRow> {
    let row: Row
    let rowNumber: Int
    private let rowMapping: [Int?]?
    private let options: ReadingOptions
    private(set) var currentIndex: Int = 0

    init(row: Row, rowNumber: Int, rowMapping: [Int?]?, options: ReadingOptions) {
        self.row = row
        self.rowNumber = rowNumber
        self.rowMapping = rowMapping
        self.options = options
    }
    
    func nextValue<T>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        guard let value = nextValueIfPresent(T.self) else {
            throw CSVDecodingError.valueNotFound(T.self, forKey: key, rowNumber: rowNumber)
        }
        return value
    }
    
    func nextValueIfPresent<T>(_ type: T.Type, isPeek: Bool = false) -> T? {
        guard currentIndex < row.count else { return nil }
        var value = row[rowMapping?[currentIndex] ?? currentIndex, type]
        if type == String.self, value == nil {
            value = "" as? T
        }
        currentIndex += 1
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

    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey? = nil, decoding: DataDecoder) throws -> T {
        if let parser = options.parserForType(type) {
            let string = try decoding.nextString(forKey: key)
            return try parse(type, string: string, forKey: key, parser: parser)
        } else {
            do {
                return try T(from: decoding)
            } catch CodableStringError.invalidFormat(let string) {
                throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
            }
        }
    }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, decoding: DataDecoder) throws -> T? {
        if let parser = options.parserForType(type) {
            guard let string = decoding.nextStringIfPresent(), !string.isEmpty else { return nil }
            return try parse(type, string: string, parser: parser)
        } else {
            guard let string = decoding.peekStringIfPresent(), !string.isEmpty else { return nil }
            do {
                return try T(from: decoding)
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
    
    func decode<T: LosslessStringConvertible>(_ type: T.Type, forKey key: CodingKey? = nil) throws -> T {
        let string = try nextString()
        guard let value = T(string) else {
            throw CSVDecodingError.dataCorrupted(string: string, forKey: key, rowNumber: rowNumber)
        }
        return value
    }

    func decodeIfPresent<T: LosslessStringConvertible>(_ type: T.Type) throws -> T? {
        guard let string = nextStringIfPresent(), !string.isEmpty else { return nil }
        guard let value = T(string) else {
            throw CSVDecodingError.dataCorrupted(string: string, rowNumber: rowNumber)
        }
        return value
    }
}
