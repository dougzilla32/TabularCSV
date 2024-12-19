//
//  WritingOptions.swift
//  SweepMap
//
//  Created by Doug on 12/11/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public struct WritingOptions {
    public var csvWritingOptions: CSVWritingOptions
    
    /// A Boolean value that indicates whether to write a header with the column names.
    ///
    /// Defaults to `true`.
    public var includesHeader: Bool {
        get { csvWritingOptions.includesHeader }
        set { csvWritingOptions.includesHeader = newValue }
    }
    
    /// A closure that maps dates to their string representations.
    ///
    /// Defaults to ISO 8601 encoding.
    public var dateFormatter: (Date) -> String {
        get {
            if #available(macOS 12.3, iOS 15.4, tvOS 15.4, watchOS 8.5, *) {
                return csvWritingOptions.dateFormatter
            } else {
                let noop: (Date) -> String = { _ in "" }
                return _dateFormatter ?? noop
            }
        }
        set {
            if #available(macOS 12.3, iOS 15.4, tvOS 15.4, watchOS 8.5, *) {
                csvWritingOptions.dateFormatter = newValue
            } else {
                _dateFormatter = newValue
            }
        }
    }
    
    private var _dateFormatter: ((Date) -> String)?
    
    /// The string the CSV file generator uses to represent nil values.
    ///
    /// Defaults to an empty string.
    public var nilEncoding: String {
        get { csvWritingOptions.nilEncoding }
        set { csvWritingOptions.nilEncoding = newValue }
    }
    
    /// The string the CSV file generator uses to represent true Boolean values.
    ///
    /// Defaults to `true`.
    public var trueEncoding: String {
        get { csvWritingOptions.trueEncoding }
        set { csvWritingOptions.trueEncoding = newValue }
    }
    
    /// The string the CSV file generator uses to represent false Boolean values.
    ///
    /// Defaults to `false`.
    public var falseEncoding: String {
        get { csvWritingOptions.falseEncoding }
        set { csvWritingOptions.falseEncoding = newValue }
    }
    
    /// The string the CSV file generator uses to represent a newline sequence.
    ///
    /// Defaults to a line feed.
    public var newline: String {
        get { csvWritingOptions.newline }
        set { csvWritingOptions.newline = newValue }
    }
    
    /// The character the CSV file generator uses to separate data fields in a CSV file.
    ///
    /// Defaults to comma (`,`).
    public var delimiter: Character {
        get { csvWritingOptions.delimiter }
        set { csvWritingOptions.delimiter = newValue }
    }
    
    public var dataFormatter: ((Data) -> String)?
    
    public var decimalFormatter: ((Decimal) -> String)?
    
    public var urlFormatter: ((URL) -> String)?
    
    public init(
        includesHeader: Bool = true,
        nilEncoding: String = "",
        trueEncoding: String = "true",
        falseEncoding: String = "false",
        newline: String = "\n",
        delimiter: Character = ",",
        dataFormatter: ((Data) -> String)? = nil,
        decimalFormatter: ((Decimal) -> String)? = nil,
        urlFormatter: ((URL) -> String)? = nil
    ) {
        csvWritingOptions = CSVWritingOptions(
            includesHeader: includesHeader,
            nilEncoding: nilEncoding,
            trueEncoding: trueEncoding,
            falseEncoding: falseEncoding,
            newline: newline,
            delimiter: delimiter
        )
        self.dataFormatter = dataFormatter
        self.decimalFormatter = decimalFormatter
        self.urlFormatter = urlFormatter
    }

    public init() { csvWritingOptions = .init() }
    
    func encode<T: Encodable>(_ type: T.Type, value: T, rowNumber: Int, encoding: Encoder) throws {
        if let formatter = formatterForType(type) {
            try formatter(value).encode(to: encoding)
        } else {
            try value.encode(to: encoding)
        }
    }

    func encodeIfPresent<T: Encodable>(_ type: T.Type, value: T, rowNumber: Int, encoding: Encoder) throws {
        if let formatter = formatterForType(type) {
            try formatter(value).encode(to: encoding)
        } else {
            try value.encode(to: encoding)
        }
    }

    private func formatterForType<T>(_ type: T.Type) -> ((T) -> String)? {
        switch type {
        case is Data.Type:
            guard let dataFormatter = dataFormatter else { return nil }
            return { dataFormatter($0 as! Data) }
        case is Decimal.Type:
            guard let decimalFormatter = decimalFormatter else { return nil }
            return { decimalFormatter($0 as! Decimal) }
        case is URL.Type:
            guard let urlFormatter = urlFormatter else { return nil }
            return { urlFormatter($0 as! URL) }
        default:
            return nil
        }
    }
}
