//
//  TabularCSVWriter.swift
//  TabularCSV
//
//  Created by Doug on 12/19/24.
//

import Foundation
import TabularData

public struct TabularCSVWriter {
    private let options: WritingOptions

    public init(options: WritingOptions = .init()) {
        self.options = options
    }
    
    public init(options: ((inout WritingOptions) -> Void)) {
        var opts = WritingOptions()
        options(&opts)
        self.init(options: opts)
    }
    
    public func write<T: Encodable>(_ values: [T], header: [String]?, toPath filePath: String) throws {
        let includesHeader = self.options.includesHeader(header != nil)
        let fileURL = URL(fileURLWithPath: filePath)
        let columns = try DataFrameEncoder(options: includesHeader.writingOptions).encode(values, header: header)
        try DataFrame(columns: columns).writeCSV(to: fileURL, options: includesHeader.csvWritingOptions)
    }
    
    public func csvRepresentation<T: Encodable>(_ values: [T], header: [String]?) throws -> Data {
        let includesHeader = self.options.includesHeader(header != nil)
        let columns = try DataFrameEncoder(options: includesHeader.writingOptions).encode(values, header: header)
        return try DataFrame(columns: columns).csvRepresentation(options: includesHeader.csvWritingOptions)
    }
    
    public func write<T: KeyedEncodable>(_ values: [T], includesHeader: Bool = true, toPath filePath: String) throws {
        let header = T.CodingKeysType.allCases.map { $0.rawValue }
        try write(values, header: header, toPath: filePath)
    }
    
    public func csvRepresentation<T: KeyedEncodable>(_ values: [T], includesHeader: Bool = true) throws -> Data {
        let header = T.CodingKeysType.allCases.map { $0.rawValue }
        return try csvRepresentation(values, header: header)
    }
}
