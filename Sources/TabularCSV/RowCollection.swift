//
//  RowCollection.swift
//  TabularCSV
//
//  Created by Doug on 12/16/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

// MARK: Data rows and columns

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


struct CSVField: Hashable {
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
