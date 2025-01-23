//
//  TabularDecoder.swift
//  TabularCSV
//
//  Created by Doug on 12/5/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

class DataValue {
    var data: Data
    
    init() { data = Data() }

    init(data: Data) { self.data = data }
}

enum FileOrData {
    case file(String)
    case data(DataValue)
    
    init(data: Data) {
        self = .data(.init(data: data))
    }
}

//public struct StreamingTask: Sendable {
//    func start() { }
//    func stop() { }
//    func cancel() { }
//}

public struct TabularDecoder {
    //    public static func streamIn<T: Codable>(
    //        type: T.Type,
    //        header: [String],
    //        atPath filePath: String,
    //        options: ReadingOptions = .init(),
    //        completionHandler: @escaping @Sendable (T?, Bool, (any Error)?) -> Void) throws -> StreamingTask
    //    {
    //        let columnTypes = try TabularCSV.determineColumnTypes(type: type, header: header, from: filePath, options: options)
    //        let dataFrame = try DataFrame(contentsOfCSVFile: URL(fileURLWithPath: filePath), types: columnTypes, options: options.tabularOptions)
    //        let headerIndexMap: [Int?]? =
    //            options.hasHeaderRow
    //            ? try createHeaderIndexMap(for: T.self, expectedHeader: header, csvHeader: dataFrame.columns.map(\.name))
    //            : nil
    //    }
    
    //    public static func streamInAsync<T: Codable & Sendable>(
    //        type: T.Type,
    //        header: [String],
    //        atPath filePath: String,
    //        options: ReadingOptions = .init()) -> AsyncThrowingStream<T, any Error>
    //    {
    //        AsyncThrowingStream { continuation in
    //            do {
    //                let task = try streamIn(type: type, header: header, atPath: filePath, options: options) { data, isEOF, error in
    //                    if let error = error {
    //                        continuation.finish(throwing: error)
    //                    }
    //                    if let data = data {
    //                        continuation.yield(data)
    //                    }
    //                    if isEOF {
    //                        continuation.finish()
    //                    }
    //                }
    //                continuation.onTermination = { _ in
    //                    task.stop()
    //                }
    //                task.start()
    //            } catch {
    //                continuation.finish(throwing: error)
    //            }
    //        }
    //    }
    
    private let options: ReadingOptions

    public init(options: ReadingOptions = .init()) {
        self.options = options
    }
    
    public init(options: ((inout ReadingOptions) -> Void)) {
        var opts = ReadingOptions()
        options(&opts)
        self.init(options: opts)
    }
    
    public func read<T: Decodable & Collection>(
        _ type: T.Type,
        fromPath filePath: String,
        hasHeaderRow: Bool = true,
        overrideHeader: [String]? = nil) throws -> (rows: T, header: [String])
    {
        try read(type, fileOrData: FileOrData.file(filePath), hasHeaderRow: hasHeaderRow, overrideHeader: overrideHeader)
    }
    
    public func read<T: Decodable & Collection>(
        _ type: T.Type,
        csvData: Data,
        hasHeaderRow: Bool = true,
        overrideHeader: [String]? = nil) throws -> (rows: T, header: [String])
    {
        try read(type, fileOrData: FileOrData(data: csvData), hasHeaderRow: hasHeaderRow, overrideHeader: overrideHeader)
    }
    
    private func read<T: Decodable & Collection>(
        _ type: T.Type,
        fileOrData: FileOrData,
        hasHeaderRow: Bool,
        overrideHeader: [String]?) throws -> (rows: T, header: [String])
    {
        let columnInfo = try introspectColumns(type, fileOrData: fileOrData, hasHeaderRow: hasHeaderRow, overrideHeader: overrideHeader)
        let headerOptions = options.hasHeaderRow(hasHeaderRow)

        let dataFrame: DataFrame
        switch fileOrData {
        case .file(let filePath):
            dataFrame = try DataFrame(contentsOfCSVFile: URL(fileURLWithPath: filePath), types: columnInfo.types, options: headerOptions.csvReadingOptions)
        case .data(let dataValue):
            dataFrame = try DataFrame(csvData: dataValue.data, types: columnInfo.types, options: headerOptions.csvReadingOptions)
        }

        let dataFrameDecoder = DataFrameDecoder(options: options)
        let rows = try dataFrameDecoder.decode(type, rows: dataFrame.rows, columns: dataFrame.columns, header: columnInfo.header)
        return (rows: rows, header: overrideHeader ?? dataFrame.columns.map(\.name))
    }
    
    struct ColumnInfo {
        let header: [String]
        let types: [String: CSVType]
    }
    
    private func introspectColumns<T: Decodable & Collection>(
        _ type: T.Type,
        fileOrData: FileOrData,
        hasHeaderRow: Bool,
        overrideHeader: [String]?) throws -> ColumnInfo
    {
        let firstPart: (data: Data, lines: [String])
        do {
            let numLinesToRead = hasHeaderRow ? 2 : 1
            switch fileOrData {
            case .file(let filePath):
                firstPart = try TabularDecoder.readLines(from: filePath, limit: numLinesToRead)
            case .data(let dataValue):
                firstPart = try TabularDecoder.convertLines(from: dataValue.data, limit: numLinesToRead)
            }
            guard firstPart.lines.count == numLinesToRead else {
                return ColumnInfo(header: [], types: [:])
            }
        }

        let headerOptions = options.hasHeaderRow(hasHeaderRow)
        let dataFrame = try DataFrame(csvData: firstPart.data, options: headerOptions.csvReadingOptions)
        let csvHeader = hasHeaderRow ? dataFrame.columns.map(\.name) : nil

        let types: [String: CSVType]
        do {
            let typesHeader = csvHeader ?? (0..<dataFrame.rows[0].count).map { "Column \($0)" }
            types = Dictionary(uniqueKeysWithValues: typesHeader.map { ($0, .string) })
        }

        let header = overrideHeader ?? (hasHeaderRow ? dataFrame.columns.map(\.name) : nil)
        if let header {
            return ColumnInfo(header: header, types: types)
        }
        
        let typeDecoder = StringDecoder(options: options)
        let row: [String?] = dataFrame.rows[0].map {
            if let value = $0 { String(describing: value) } else { nil }
        }
        let typeDecoderResult: (value: T, fields: OrderedSet<CSVField>)
        do {
            typeDecoderResult = try typeDecoder.decodeTypes(type, rows: [row], columns: StringColumns(), header: csvHeader)
        } catch {
            throw DataDecodingError.headerIsNeeded(error: error)
        }
        
        return ColumnInfo(
            header: typeDecoderResult.fields.all().map(\.name),
            types: types)
    }


    enum FileError: Error {
        case open
    }

    private static func readLines(from filePath: String, limit: Int) throws -> (data: Data, lines: [String]) {
        var string = ""
        var lines = [String]()
        var count = 0
        
        guard let file = fopen(filePath, "r") else {
            throw FileError.open
        }

        var linePointer: UnsafeMutablePointer<CChar>? = nil
        var lineCap: Int = 0

        while getline(&linePointer, &lineCap, file) > 0 {
            if let linePointer = linePointer {
                let line = String(cString: linePointer)
                string.append(line)
                lines.append(line)
                count += 1
                if count == limit {
                    break
                }
            }
        }

        free(linePointer)
        fclose(file)

        return (data: string.data(using: .utf8)!, lines: lines)
    }
    
    enum DataError: Error {
        case decoding
    }

    private static func convertLines(from data: Data, limit: Int, encoding: String.Encoding = .utf8) throws -> (data: Data, lines: [String]) {
        // Buffer to hold the partial result
        var lines: [String] = []

        // Read the data byte by byte
        let newLineChar = UInt8(ascii: "\n")
        var shortenedData = Data()
        var currentLine = Data()

        for byte in data {
            // Append byte to the current line
            shortenedData.append(byte)

            // Check if this is a newline character
            if byte == newLineChar {
                // Convert the line to a string
                if let line = String(data: currentLine, encoding: encoding) {
                    lines.append(line)

                    // Stop if we have collected enough lines
                    if lines.count == limit {
                        break
                    }

                    // Clear the buffer for the next line
                    currentLine.removeAll()
                } else {
                    // Handle decoding error (e.g., invalid UTF-8 sequence)
                    throw DataError.decoding
                }
            } else {
                currentLine.append(byte)
            }
        }

        // Add any remaining line if we stopped before reaching the limit
        if lines.count < limit, !currentLine.isEmpty {
            if let line = String(data: currentLine, encoding: encoding) {
                lines.append(line)
            } else {
                throw DataError.decoding
            }
        }

        return (data: shortenedData, lines: lines)
    }
}
