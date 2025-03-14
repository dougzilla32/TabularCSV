//
//  TabularEncoder.swift
//  TabularCSV
//
//  Created by Doug on 12/19/24.
//

import Foundation
import TabularData

public struct TabularEncoder {
    private let options: WritingOptions

    public init(options: WritingOptions = .init()) {
        self.options = options
    }
    
    public init(options: ((inout WritingOptions) -> Void)) {
        var opts = WritingOptions()
        options(&opts)
        self.init(options: opts)
    }
    
    public func write<T: Encodable & Collection>(
        _ value: T,
        to file: URL,
        includesHeader: Bool = true,
        overrideHeader: [String]? = nil) throws where T.Element: Encodable
    {
        try write(value, fileOrData: .file(file), includesHeader: includesHeader, overrideHeader: overrideHeader)
    }
    
    public func csvRepresentation<T: Encodable & Collection>(
        _ value: T,
        includesHeader: Bool = true,
        overrideHeader: [String]? = nil) throws -> Data where T.Element: Encodable
    {
        let dataValue = DataValue()
        try write(value, fileOrData: .data(dataValue), includesHeader: includesHeader, overrideHeader: overrideHeader)
        return dataValue.data
    }

    private func write<T: Encodable & Collection>(
        _ value: T,
        fileOrData: FileOrData,
        includesHeader: Bool,
        overrideHeader: [String]?) throws where T.Element: Encodable
    {
        let header = try overrideHeader ?? introspectColumns(value)
        let headerOptions = self.options.includesHeader(includesHeader)
        
        let dataFrameEncoder = DataFrameEncoder(options: headerOptions.writingOptions)
        let columns = try dataFrameEncoder.encode(value, header: header).map { $0.eraseToAnyColumn() }

        switch fileOrData {
        case .file(let file):
            try DataFrame(columns: columns).writeCSV(to: file, options: headerOptions.csvWritingOptions)
        case .data(let csvData):
            csvData.data = try DataFrame(columns: columns).csvRepresentation(options: headerOptions.csvWritingOptions)
        }
    }

    private func introspectColumns<T: Encodable & Collection>(
        _ value: T) throws -> [String] where T.Element: Encodable
    {
        let typeEncoder = StringEncoder(options: options)
        let typeEncoderResult = try typeEncoder.encodeWithHeaderAndTypes([value.first], header: nil)
        return typeEncoderResult.fields.all().map(\.name)
    }
}
