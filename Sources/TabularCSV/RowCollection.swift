//
//  RowCollection.swift
//  TabularCSV
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

// MARK: Data rows and columns protocols

public protocol DataRow {
    var count: Int { get }
    subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? { get }
    func isNil(at position: Int) -> Bool
}

public protocol DataRows: Collection where Element: DataRow { }

public protocol DataColumns {
    func type(at position: Int) -> CSVPrimitive.Type
}

// MARK: DataFrame rows and columns

extension DataFrame.Row: DataRow {
    public subscript<T: LosslessStringConvertible>(position: Int, type: T.Type, options: ReadingOptions) -> T? {
        self[position, type]
    }
    
    public func isNil(at position: Int) -> Bool {
        self[position] == nil
    }
}

extension DataFrame.Rows: DataRows { }

extension Array: DataColumns where Element == AnyColumn {
    public func type(at position: Int) -> CSVPrimitive.Type {
        self[position].wrappedElementType as! CSVPrimitive.Type
    }
}

// MARK: String rows and columns

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
    
    public func isNil(at position: Int) -> Bool {
        self[position] == nil
    }
}

extension Array: DataRows where Element == [String?] { }

public struct StringColumns: DataColumns {
    public func type(at position: Int) -> CSVPrimitive.Type {
        String.self
    }
}

// MARK: CSVField

struct CSVField: Hashable {
    let name: String
    let type: CSVType
}
