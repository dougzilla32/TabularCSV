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
    case file(URL)
    case data(DataValue)
    
    init(data: Data) {
        self = .data(.init(data: data))
    }
}

struct ColumnInfo {
    let header: [String]?
    let types: [String: CSVType]?
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
        from file: URL,
        hasHeaderRow: Bool = true,
        overrideHeader: [String]? = nil) throws -> (rows: T, header: [String]?)
    {
        try read(type, fileOrData: FileOrData.file(file), hasHeaderRow: hasHeaderRow, overrideHeader: overrideHeader)
    }
    
    public func read<T: Decodable & Collection>(
        _ type: T.Type,
        csvData: Data,
        hasHeaderRow: Bool = true,
        overrideHeader: [String]? = nil) throws -> (rows: T, header: [String]?)
    {
        try read(type, fileOrData: FileOrData(data: csvData), hasHeaderRow: hasHeaderRow, overrideHeader: overrideHeader)
    }
    
    private func read<T: Decodable & Collection>(
        _ type: T.Type,
        fileOrData: FileOrData,
        hasHeaderRow: Bool,
        overrideHeader: [String]?) throws -> (rows: T, header: [String]?)
    {
        let columnInfo = try introspectColumns(type, fileOrData: fileOrData, hasHeaderRow: hasHeaderRow, overrideHeader: overrideHeader)
        let headerOptions = options.hasHeaderRow(hasHeaderRow)

        let dataFrame: DataFrame
        switch fileOrData {
        case .file(let file):
            dataFrame = try DataFrame(contentsOfCSVFile: file, types: columnInfo.types ?? [:], options: headerOptions.csvReadingOptions)
        case .data(let dataValue):
            dataFrame = try DataFrame(csvData: dataValue.data, types: columnInfo.types ?? [:], options: headerOptions.csvReadingOptions)
        }

        let dataFrameDecoder = DataFrameDecoder(options: options)
        let rows = try dataFrameDecoder.decode(type, rows: dataFrame.rows, header: columnInfo.header ?? [])
        return (rows: rows, header: columnInfo.header)
    }
    
    private func introspectColumns<T: Decodable & Collection>(
        _ type: T.Type,
        fileOrData: FileOrData,
        hasHeaderRow: Bool,
        overrideHeader: [String]?) throws -> ColumnInfo
    {
        let firstPart: (data: Data, lines: [String]) = try {
            let limit = hasHeaderRow ? 2 : 1
            switch fileOrData {
            case .file(let file):
                return try TabularDecoder.readLines(from: file, limit: limit)
            case .data(let dataValue):
                return try TabularDecoder.convertLines(from: dataValue.data, limit: limit)
            }
        }()

        guard !firstPart.lines.isEmpty else {
            return ColumnInfo(header: nil, types: nil)
        }

        let dataFrame = try DataFrame(csvData: firstPart.data, options: options.hasHeaderRow(hasHeaderRow).csvReadingOptions)
        let csvHeader = hasHeaderRow ? dataFrame.columns.map(\.name) : nil

        let types: [String: CSVType] = {
            let typesHeader = csvHeader ?? (0..<dataFrame.rows[0].count).map { "Column \($0)" }
            return Dictionary(uniqueKeysWithValues: typesHeader.map { ($0, .string) })
        }()

        if let header = overrideHeader ?? csvHeader {
            return ColumnInfo(header: header, types: types)
        }

        // There is no override header and there is no header in the csv file, so we need to
        // introspect the header from the Decodable.
        let typeDecoderResult = try {
            let row = dataFrame.rows[0].map { $0.map(String.init(describing:)) }
            let typeDecoder = StringDecoder(options: options)
            do {
                return try typeDecoder.decodeTypes(type, rows: [row], header: csvHeader)
            } catch {
                throw DataDecodingError.headerIsNeeded(error: error)
            }
        }()

        return ColumnInfo(
            header: typeDecoderResult.fields.all().map(\.name),
            types: types
        )
    }

    enum FileError: Error {
        case open
    }

    private static func readLines(from file: URL, limit: Int) throws -> (data: Data, lines: [String]) {
        var string = ""
        var lines = [String]()
        var count = 0
        
        guard let file = fopen(file.path, "r") else {
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
