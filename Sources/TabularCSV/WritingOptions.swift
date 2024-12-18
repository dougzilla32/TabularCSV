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
    public var tabularOptions: CSVWritingOptions
    
    /// A Boolean value that indicates whether to write a header with the column names.
    ///
    /// Defaults to `true`.
    public var includesHeader: Bool {
        get { tabularOptions.includesHeader }
        set { tabularOptions.includesHeader = newValue }
    }
    
    /// A closure that maps dates to their string representations.
    ///
    /// Defaults to ISO 8601 encoding.
    public var dateFormatter: (Date) -> String {
        get {
            if #available(macOS 12.3, iOS 15.4, tvOS 15.4, watchOS 8.5, *) {
                return tabularOptions.dateFormatter
            } else {
                let noop: (Date) -> String = { _ in "" }
                return _dateFormatter ?? noop
            }
        }
        set {
            if #available(macOS 12.3, iOS 15.4, tvOS 15.4, watchOS 8.5, *) {
                tabularOptions.dateFormatter = newValue
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
        get { tabularOptions.nilEncoding }
        set { tabularOptions.nilEncoding = newValue }
    }
    
    /// The string the CSV file generator uses to represent true Boolean values.
    ///
    /// Defaults to `true`.
    public var trueEncoding: String {
        get { tabularOptions.trueEncoding }
        set { tabularOptions.trueEncoding = newValue }
    }
    
    /// The string the CSV file generator uses to represent false Boolean values.
    ///
    /// Defaults to `false`.
    public var falseEncoding: String {
        get { tabularOptions.falseEncoding }
        set { tabularOptions.falseEncoding = newValue }
    }
    
    /// The string the CSV file generator uses to represent a newline sequence.
    ///
    /// Defaults to a line feed.
    public var newline: String {
        get { tabularOptions.newline }
        set { tabularOptions.newline = newValue }
    }
    
    /// The character the CSV file generator uses to separate data fields in a CSV file.
    ///
    /// Defaults to comma (`,`).
    public var delimiter: Character {
        get { tabularOptions.delimiter }
        set { tabularOptions.delimiter = newValue }
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
        tabularOptions = CSVWritingOptions(
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

    public init() { tabularOptions = .init() }
}
