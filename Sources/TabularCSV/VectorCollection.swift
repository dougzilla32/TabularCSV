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
    
    var vectors: [VectorType] { get }
    
    var numRows: Int { get }
    
    init(header: OrderedDictionary<String, Int>?, numRows: Int)
    
    mutating func nextRow()
    
    mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int)
}

public struct AnyColumnMatrix: DataMatrix {
    public typealias VectorType = Column<String>
    
    public private(set) var vectors: [VectorType]
    private var currentRow: Int
    private let header: OrderedDictionary<String, Int>?
    public let numRows: Int

    public init(header: OrderedDictionary<String, Int>?, numRows: Int) {
        self.vectors = []
        self.currentRow = -1
        self.header = header
        self.numRows = numRows
        
        if let headerNames = header?.orderedKeys {
            for index in 0..<headerNames.count {
                var column = Column<String>(name: headerNames[index], capacity: numRows)
                column.append(contentsOf: Array<String>(repeating: "", count: numRows))
                vectors.append(column)
            }
        }
    }
    
    public mutating func nextRow() {
        currentRow += 1
    }
    
    public mutating func encode<T: CSVPrimitive>(_ value: T?, index: Int) {
        let string = value.map { String($0) } ?? ""
        if header != nil {
            vectors[index][currentRow] = string
        } else {
            ensureMinimumSize(index + 1)
            vectors[index].append(string)
        }
    }
    
    private mutating func ensureMinimumSize(_ count: Int) {
        for index in vectors.count..<count {
            vectors.append(Column<String>(name: "Column \(index)", capacity: numRows))
        }
    }
}

public struct StringMatrix: DataMatrix {
    public typealias VectorType = [String?]
    
    public private(set) var vectors: [VectorType]
    private let header: OrderedDictionary<String, Int>?
    public let numRows: Int
    
    public init(header: OrderedDictionary<String, Int>?, numRows: Int) {
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
}
