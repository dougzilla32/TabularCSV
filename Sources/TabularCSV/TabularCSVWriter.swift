//
//  TabularCSVWriter.swift
//  TabularCSV
//
//  Created by Doug on 12/19/24.
//

import Foundation

public struct TabularCSVWriter<RowType: Encodable> {
    private let rows: [RowType]
    private let header: [String]
    private let fileURL: URL
    private let options: WritingOptions

    init(rows: [RowType], header: [String], toPath filePath: String, options: WritingOptions = .init()) {
        self.rows = rows
        self.header = header
        self.fileURL = URL(fileURLWithPath: filePath)
        self.options = options
    }
    
    init(rows: [RowType], header: [String], toPath filePath: String, options: ((inout WritingOptions) -> Void)) {
        var opts = WritingOptions()
        options(&opts)
        self.init(rows: rows, header: header, toPath: filePath, options: opts)
    }
    
    init<T: EncodableRow>(rows: [T], toPath filePath: String, options: WritingOptions = .init()) {
        self.rows = rows.map { $0 as! RowType }
        self.header = T.CodingKeysType.allCases.map { $0.rawValue }
        self.fileURL = URL(fileURLWithPath: filePath)
        self.options = options
    }
    
    init<T: EncodableRow>(rows: [T], toPath filePath: String, options: ((inout WritingOptions) -> Void)) {
        var opts = WritingOptions()
        options(&opts)
        self.init(rows: rows, toPath: filePath, options: opts)
    }
    
    public func write() throws {
        let dataFrame = try DataFrameEncoder(options: options).encode(header: header, values: rows)
        try dataFrame.writeCSV(to: fileURL, options: options.csvWritingOptions)
    }
}
