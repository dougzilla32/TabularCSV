//
//  RowCollection.swift
//  SweepMap
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import TabularData

final class ColumnCollection {
    let header: [String]?
    let numRows: Int
    var columns: [AnyColumn] = []
    private let options: WritingOptions
    var columnIndex = 0
    
    init(header: [String]?, numRows: Int, options: WritingOptions) {
        self.header = header
        self.numRows = numRows
        self.options = options
    }
    
    func encode<T>(_ value: T) {
        if columns.count == columnIndex {
            columns.append(Column<T>(name: header?[safe: columnIndex] ?? "Column \(columnIndex)", capacity: numRows).eraseToAnyColumn())
        }
        columns[columnIndex].append(value)
        columnIndex += 1
    }
    
    func encode<T: Encodable>(_ type: T.Type, value: T, rowNumber: Int, encoding: Encoder) throws {
        try options.encode(type, value: value, rowNumber: rowNumber, encoding: encoding)
    }

    func encodeIfPresent<T: Encodable>(_ type: T.Type, value: T, rowNumber: Int, encoding: Encoder) throws {
        try options.encodeIfPresent(type, value: value, rowNumber: rowNumber, encoding: encoding)
    }
}

extension Array {
    subscript(safe index: Int) -> Element? {
        return indices.contains(index) ? self[index] : nil
    }
}
