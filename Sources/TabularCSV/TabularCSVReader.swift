//
//  TabularCSVReader.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public typealias KeyedCodable = KeyedDecodable & KeyedEncodable

public protocol KeyedDecodable: Decodable {
    associatedtype CodingKeysType: CaseIterable & RawRepresentable where CodingKeysType.RawValue == String

    var row: Int { get set }
    func postInit()
}

public extension KeyedDecodable {
    var row: Int { get { -1 } set { } }
    func postInit() { }
}

public protocol KeyedEncodable: Encodable {
    associatedtype CodingKeysType: CaseIterable & RawRepresentable where CodingKeysType.RawValue == String
}

public enum FileError: Error {
    case open
}

public enum DataError: Error {
    case decoding
}

//public struct StreamingTask: Sendable {
//    func start() { }
//    func stop() { }
//    func cancel() { }
//}

public struct TabularCSVReader {
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
    
    private enum DataSource {
        case file(String)
        case data(Data)
    }

    private let options: ReadingOptions

    public init(options: ReadingOptions = .init()) {
        self.options = options
    }
    
    public init(options: ((inout ReadingOptions) -> Void)) {
        var opts = ReadingOptions()
        options(&opts)
        self.init(options: opts)
    }
    
    public func read<T: Decodable>(_ type: T.Type, header: [String]?, fromPath filePath: String) throws -> [T] {
        try read(type, header: header, dataSource: DataSource.file(filePath))
    }
    
    public func read<T: Decodable>(_ type: T.Type, header: [String]?, csvData: Data) throws -> [T] {
        try read(type, header: header, dataSource: DataSource.data(csvData))
    }
    
    public func read<T: KeyedDecodable>(_ type: T.Type, hasHeaderRow: Bool = true, fromPath filePath: String) throws -> [T] {
        return try read(type, hasHeaderRow: hasHeaderRow, dataSource: DataSource.file(filePath))
    }
    
    public func read<T: KeyedDecodable>(_ type: T.Type, hasHeaderRow: Bool = true, csvData: Data) throws -> [T] {
        return try read(type, hasHeaderRow: hasHeaderRow, dataSource: DataSource.data(csvData))
    }
    
    private func read<T: Decodable>(_ type: T.Type, header: [String]?, dataSource: DataSource) throws -> [T] {
        let columnTypes = try determineColumnTypes(type, header: header, dataSource: dataSource)
        let hasHeaderRow = options.hasHeaderRow(header != nil)
        let dataFrame: DataFrame

        switch dataSource {
        case .file(let filePath):
            dataFrame = try DataFrame(contentsOfCSVFile: URL(fileURLWithPath: filePath), types: columnTypes, options: hasHeaderRow.csvReadingOptions)
        case .data(let csvData):
            dataFrame = try DataFrame(csvData: csvData, types: columnTypes, options: hasHeaderRow.csvReadingOptions)
        }

        let rowMapping: [Int?]? = try createHeaderIndexMap(expectedHeader: header, csvHeader: dataFrame.columns.map(\.name))
        let dataFrameDecoder = DataFrameDecoder(options: options)
        return try dataFrameDecoder.decode(type, dataFrame: dataFrame, rowMapping: rowMapping)
    }
    
    private func read<T: KeyedDecodable>(_ type: T.Type, hasHeaderRow: Bool, dataSource: DataSource) throws -> [T] {
        let header: [String]? = hasHeaderRow ? T.CodingKeysType.allCases.map { $0.rawValue } : nil
        var rows = try read(type, header: header, dataSource: dataSource)
        for r in 0..<rows.count {
            rows[r].row = r+1
            rows[r].postInit()
        }
        return rows
    }
    
    private func determineColumnTypes<T: Decodable>(_ type: T.Type, header: [String]?, dataSource: DataSource) throws -> [String: CSVType] {
        let numLinesToRead = header != nil ? 2 : 1
        let firstLittleBit: (data: Data, lines: [String])
        switch dataSource {
        case .file(let filePath):
            firstLittleBit = try TabularCSVReader.readLines(from: filePath, limit: numLinesToRead)
        case .data(let data):
            firstLittleBit = try TabularCSVReader.convertLines(from: data, limit: numLinesToRead)
        }
        guard firstLittleBit.lines.count == numLinesToRead else { return [:] }
        
        let stringTypes: [String: CSVType]
        if let header {
            stringTypes = Dictionary(uniqueKeysWithValues: header.map { ($0, CSVType.string) })
        } else {
            stringTypes = [:]
        }

        let hasHeaderRow = options.hasHeaderRow(header != nil)
        let dataFrame = try DataFrame(csvData: firstLittleBit.data, types: stringTypes, options: hasHeaderRow.csvReadingOptions)

        let headerNames: [String]
        let row: [String?]
        if let header {
            headerNames = dataFrame.columns.map(\.name)
            let headerIndexMap = try createHeaderIndexMap(expectedHeader: header, csvHeader: headerNames)
            row = TabularCSVReader.reorder(row: dataFrame.rows[0].map { $0 as? String ?? "" }, headersIndexMap: headerIndexMap)
        } else {
            headerNames = (0..<dataFrame.rows[0].count).map { "Column \($0)" }
            row = dataFrame.rows[0].map {
                if let value = $0 { String(describing: value) } else { nil }
            }
        }

        let typeDecoder = TypeDecoder(options: options)
        try typeDecoder.decode(type, from: row, rowNumber: 1)

        var columnTypes = [String: CSVType]()
        zip(headerNames, typeDecoder.types.csvTypes).forEach {
            columnTypes[$0] = $1
        }
        return columnTypes
    }
    
    private func createHeaderIndexMap(expectedHeader: [String]?, csvHeader: [String]) throws -> [Int?]? {
        guard let expectedHeader else { return nil }
        
        // Find unexpected headers
//        let unexpectedHeader = csvHeader.filter { !expectedHeader.contains($0) }
//        
        // Throw an error if unexpected headers are found
//        if !unexpectedHeader.isEmpty {
//            throw NSError(
//                domain: "CSVParsingError",
//                code: 1,
//                userInfo: [NSLocalizedDescriptionKey: "Unexpected header found: \(unexpectedHeader)"]
//            )
//        }
        
        // Map headers to their indices
        return expectedHeader.map { expectedHeaderName in
            csvHeader.firstIndex(of: expectedHeaderName)
        }
    }

    private static func reorder(row: [String], headersIndexMap: [Int?]?) -> [String?] {
        guard let headersIndexMap else { return row }

        return headersIndexMap.map { index in
            guard let index = index, index < row.count else { return nil }
            return row[index]
        }
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
