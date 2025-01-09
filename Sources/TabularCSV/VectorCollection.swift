//
//  VectorCollection.swift
//  TabularCSV
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import TabularData

public protocol DataMatrix {
    associatedtype VectorType
    
    var numRows: Int { get }
    
    init(header: [String]?, numRows: Int, transform: RowTransform)
    
    mutating func nextRow()
    
    mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int)
    
    func getVectors() -> [VectorType]
}

public struct AnyColumnMatrix: DataMatrix {
    public typealias VectorType = AnyColumn
    
    private var vectors: [VectorType?]
    private let header: [String]?
    public let numRows: Int

    public init(header: [String]?, numRows: Int, transform: RowTransform) {
        self.vectors = Array(repeating: nil, count: header?.count ?? 0)
        self.header = header
        self.numRows = numRows
        
        if case .permutation(let permutation) = transform {
            for index in 0..<permutation.count {
                if permutation[index] == nil {
                    var column = Column<String?>(name: header?[index] ?? "Column \(index)", capacity: numRows)
                    column.append(contentsOf: Array<String?>(repeating: nil, count: numRows))
                    ensureMinimumLength(index: index)
                    vectors[index] = column.eraseToAnyColumn()
                }
            }
        }
    }
    
    public mutating func nextRow() { }
    
    public mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int) {
        ensureMinimumLength(index: index)
        if vectors[index] == nil {
            vectors[index] = Column<T>(name: header?[index] ?? "Column \(index)", capacity: numRows).eraseToAnyColumn()
        }
        vectors[index]!.append(value)
    }
    
    private mutating func ensureMinimumLength(index: Int) {
        let count = vectors.count
        if index >= count {
            vectors.append(contentsOf: Array(repeating: nil, count: index - count + 1))
        }
    }
    
    public func getVectors() -> [VectorType] {
        var result: [VectorType] = []
        for index in 0..<vectors.count {
            if let vector = vectors[index] {
                result.append(vector)
            } else {
                result.append(Column<String>(
                    name: header?[index] ?? "Column \(index)",
                    contents: Array<String>(repeating: "", count: numRows)
                ).eraseToAnyColumn())
            }
        }
        return result
    }
}

public struct StringMatrix: DataMatrix {
    public typealias VectorType = [String?]
    
    private var vectors: [VectorType]
    private let header: [String]?
    public let numRows: Int
    
    public init(header: [String]?, numRows: Int, transform: RowTransform) {
        self.vectors = []
        self.header = header
        self.numRows = numRows
    }
    
    public mutating func nextRow() {
        vectors.append(Array(repeating: nil, count: header?.count ?? 0))
    }
    
    public mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int) {
        ensureMinimumLength(index: index)
        let string = value.map { String($0) }
        vectors[vectors.count - 1][index] = string
    }
    
    private mutating func ensureMinimumLength(index: Int) {
        let count = vectors[vectors.count - 1].count
        if index >= count {
            vectors[vectors.count - 1].append(contentsOf: Array(repeating: nil, count: index - count + 1))
        }
    }
    
    public func getVectors() -> [VectorType] { vectors }
}

final class VectorCollection<Matrix: DataMatrix> {
    private(set) var matrix: Matrix
    private let transform: RowTransform
    private let options: WritingOptions
    var currentColumnIndex = 0
    private(set) var headerAndTypes: [HeaderAndType]?

    init(header: [String]?, numRows: Int, transform: RowTransform, options: WritingOptions) {
        self.matrix = Matrix(header: header, numRows: numRows, transform: transform)
        self.transform = transform
        self.options = options

        if case .map = transform {
            headerAndTypes = []
        }
    }
    
    func nextRow() {
        matrix.nextRow()
        currentColumnIndex = 0
    }
    
    func encodeNext<T: CSVPrimitive>(_ value: T, forKey key: CodingKey? = nil) {
        encodeNextIfPresent(value, forKey: key)
    }
    
    func encodeNextIfPresent<T: CSVPrimitive>(_ value: T?, forKey key: CodingKey? = nil) {
        let index: Int?
        switch transform {
        case .permutation(let permutation):
            index = permutation[currentColumnIndex]
        default:
            index = currentColumnIndex
        }

        if let index {
            matrix.encode(value, index: index)
        }
        if let key {
            headerAndTypes?.append(.init(name: key.stringValue, type: T.csvType))
        }
        currentColumnIndex += 1
    }
    
    func encodeNext<T: Encodable>(_ value: T, forKey key: CodingKey? = nil, encoder: Encoder) throws {
        if let key {
            headerAndTypes?.append(.init(name: key.stringValue, type: .string))
        }
        if let formatter = options.formatterForType(T.self) {
            try formatter(value).encode(to: encoder)
        } else {
            try value.encode(to: encoder)
        }
    }

    func encodeNextIfPresent<T: Encodable>(_ value: T?, forKey key: CodingKey? = nil, encoder: Encoder) throws {
        if let key {
            headerAndTypes?.append(.init(name: key.stringValue, type: .string))
        }
        if let value, let formatter = options.formatterForType(T.self) {
            try formatter(value).encode(to: encoder)
        } else {
            try value.encode(to: encoder)
        }
    }
}
