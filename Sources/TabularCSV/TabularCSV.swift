//
//  TabularCSV.swift
//  SweepMap
//
//  Created by Doug on 12/5/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public typealias CodableRow = DecodableRow & EncodableRow

public protocol DecodableRow: Decodable {
    associatedtype CodingKeysType: CaseIterable & RawRepresentable where CodingKeysType.RawValue == String

    var row: Int { get set }
    func postInit()
}

public extension DecodableRow {
    var row: Int { get { -1 } set { } }
    func postInit() { }
}

public protocol EncodableRow: Encodable {
    associatedtype CodingKeysType: CaseIterable & RawRepresentable where CodingKeysType.RawValue == String
}

public enum FileError: Error {
    case open
}

//public struct StreamingTask: Sendable {
//    func start() { }
//    func stop() { }
//    func cancel() { }
//}

public struct TabularCSV {
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
    
    public static func importCSV<T: Decodable>(
        type: T.Type,
        header: [String],
        atPath filePath: String,
        options: ReadingOptions = .init()) throws -> [T]
    {
        let columnTypes = try TabularCSV.determineColumnTypes(type: type, header: header, from: filePath, options: options)
        let dataFrame = try DataFrame(contentsOfCSVFile: URL(fileURLWithPath: filePath), types: columnTypes, options: options.tabularOptions)
        
        let rowMapping: [Int?]? =
            options.hasHeaderRow
            ? try createHeaderIndexMap(for: T.self, expectedHeader: header, csvHeader: dataFrame.columns.map(\.name))
            : nil

        return try DataFrameDecoder(options: options).decode(T.self, dataFrame: dataFrame, rowMapping: rowMapping)
    }
    
    public static func importCSV<T: Decodable>(
        type: T.Type,
        header: [String],
        atPath filePath: String,
        options: ((inout ReadingOptions) -> Void)) throws -> [T]
    {
        var opts = ReadingOptions()
        options(&opts)
        return try importCSV(type: type, header: header, atPath: filePath, options: opts)
    }

    public static func importCSV<T: DecodableRow>(
        type: T.Type,
        atPath filePath: String,
        options: ReadingOptions = .init()) throws -> [T]
    {
        var rows = try importCSV(type: type, header: T.CodingKeysType.allCases.map { $0.rawValue }, atPath: filePath, options: options)
        for r in 0..<rows.count {
            rows[r].row = r+1
            rows[r].postInit()
        }
        return rows
    }
    
    public static func importCSV<T: DecodableRow>(
        type: T.Type,
        atPath filePath: String,
        options: ((inout ReadingOptions) -> Void)) throws -> [T]
    {
        var opts = ReadingOptions()
        options(&opts)
        return try importCSV(type: type, atPath: filePath, options: opts)
    }
    
    public static func exportCSV<T: Encodable>(
        rows: [T],
        header: [String],
        toPath filePath: String,
        options: WritingOptions = .init()) throws
    {
        let dataFrame = try DataFrameEncoder(options: options).encode(header: header, values: rows)
        let url = URL(fileURLWithPath: filePath)
        try dataFrame.writeCSV(to: url, options: options.tabularOptions)
    }
    
    public static func exportCSV<T: Encodable>(
        rows: [T],
        header: [String],
        toPath filePath: String,
        options: ((inout WritingOptions) -> Void)) throws
    {
        var opts = WritingOptions()
        options(&opts)
        try exportCSV(rows: rows, header: header, toPath: filePath, options: opts)
    }
    
    public static func exportCSV<T: EncodableRow>(
        rows: [T],
        toPath filePath: String,
        options: WritingOptions = .init()) throws
    {
        let header = T.CodingKeysType.allCases.map { $0.rawValue }
        try exportCSV(rows: rows, header: header, toPath: filePath, options: options)
    }
 
    public static func exportCSV<T: EncodableRow>(
        rows: [T],
        toPath filePath: String,
        options: ((inout WritingOptions) -> Void)) throws
    {
        var opts = WritingOptions()
        options(&opts)
        try exportCSV(rows: rows, toPath: filePath, options: opts)
    }
    
    private static func determineColumnTypes<T: Decodable>(
        type: T.Type,
        header: [String],
        from filePath: String,
        options: ReadingOptions) throws -> [String: CSVType]
    {
        let numLinesToRead = options.hasHeaderRow ? 2 : 1
        let csvData = try readLines(from: filePath, limit: numLinesToRead)
        guard csvData.lines.count == numLinesToRead else { return [:] }
        
        let stringTypes: [String: CSVType] =
            options.hasHeaderRow
            ? Dictionary(uniqueKeysWithValues: header.map { ($0, CSVType.string) })
            : [:]
        let dataFrame = try DataFrame(csvData: csvData.data, types: stringTypes, options: options.tabularOptions)

        let headerNames: [String]
        let row: [String?]
        if options.hasHeaderRow {
            headerNames = dataFrame.columns.map(\.name)
            let headerIndexMap = try createHeaderIndexMap(for: T.self, expectedHeader: header, csvHeader: headerNames)
            row = reorder(row: dataFrame.rows[0].map { $0 as? String ?? "" }, headersIndexMap: headerIndexMap)
        } else {
            headerNames = (0..<dataFrame.rows[0].count).map { "Column \($0)" }
            row = dataFrame.rows[0].map {
                if let value = $0 { String(describing: value) } else { nil }
            }
        }

        let typeDecoder = TypeDecoder(options: options)
        try typeDecoder.decode(T.self, from: row, rowNumber: 1)
        
        var columnTypes = [String: CSVType]()
        zip(headerNames, typeDecoder.types.csvTypes).forEach {
            columnTypes[$0] = $1
        }
        return columnTypes
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
    
    static func createHeaderIndexMap<T: Decodable>(for type: T.Type, expectedHeader: [String], csvHeader: [String]) throws -> [Int?] {
        // Find unexpected headers
        let unexpectedHeaders = csvHeader.filter { !expectedHeader.contains($0) }
        
        // Throw an error if unexpected headers are found
        if !unexpectedHeaders.isEmpty {
            throw NSError(
                domain: "CSVParsingError",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "Unexpected headers found: \(unexpectedHeaders)"]
            )
        }
        
        // Map headers to their indices
        return expectedHeader.map { expectedHeaderName in
            csvHeader.firstIndex(of: expectedHeaderName)
        }
    }

    static func reorder(row: [String], headersIndexMap: [Int?]) -> [String?] {
        return headersIndexMap.map { index in
            guard let index = index, index < row.count else { return nil }
            return row[index]
        }
    }
}

extension String? {
    var valueOrNil: String {
        self != nil ? "\"\(self!)\"" : "nil"
    }
}

extension Int {
    var atRow: String {
        self >= 0 ? " at row \(self+1)" : ""
    }
}
