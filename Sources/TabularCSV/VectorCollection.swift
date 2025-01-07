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

    init(header: [String]?, numRows: Int)
    
    var numRows: Int { get }
    
    var vectors: [VectorType] { get }
    
    mutating func nextRow()
    
    mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int)
}

public struct AnyColumnMatrix: DataMatrix {
    public typealias VectorType = AnyColumn
    
    private let header: [String]?
    public let numRows: Int
    
    public init(header: [String]?, numRows: Int) {
        self.header = header
        self.numRows = numRows
    }
    
    public private(set) var vectors: [VectorType] = []
    
    public mutating func nextRow() { }
    
    public mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int) {
        if vectors.count == index {
            vectors.append(Column<T>(name: header?[index] ?? "Column \(index)", capacity: numRows).eraseToAnyColumn())
        }
        vectors[index].append(value)
    }
}

public struct StringMatrix: DataMatrix {
    public typealias VectorType = [String?]
    
    public let numRows: Int

    public init(header: [String]?, numRows: Int) {
        self.numRows = numRows
    }
    
    public private(set) var vectors: [VectorType] = []
    
    public mutating func nextRow() {
        vectors.append([])
    }
    
    public mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int) {
        let string = value.map { String($0) }
        vectors[vectors.count - 1].append(string)
    }
}

final class VectorCollection<Matrix: DataMatrix> {
    private(set) var matrix: Matrix
    private let options: WritingOptions
    var columnIndex = 0
    
    init(header: [String]?, numRows: Int, options: WritingOptions) {
        self.matrix = Matrix(header: header, numRows: numRows)
        self.options = options
    }
    
    func nextRow() {
        matrix.nextRow()
        columnIndex = 0
    }
    
    func encodeNext<T: CSVPrimitive>(_ value: T) {
        matrix.encode(value, index: columnIndex)
        columnIndex += 1
    }
    
    func encodeNextIfPresent<T: CSVPrimitive>(_ value: T?) {
        matrix.encode(value, index: columnIndex)
        columnIndex += 1
    }
    
    func encodeNext<T: Encodable>(_ value: T, encoder: Encoder) throws {
        if let formatter = options.formatterForType(T.self) {
            try formatter(value).encode(to: encoder)
        } else {
            try value.encode(to: encoder)
        }
    }

    func encodeNextIfPresent<T: Encodable>(_ value: T?, encoder: Encoder) throws {
        if let value, let formatter = options.formatterForType(T.self) {
            try formatter(value).encode(to: encoder)
        } else {
            try value.encode(to: encoder)
        }
    }
}
