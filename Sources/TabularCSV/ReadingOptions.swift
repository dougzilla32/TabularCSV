//
//  ReadingOptions.swift
//  TabularCSV
//
//  Created by Doug on 12/11/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public struct ReadingOptions {
    var csvReadingOptions: CSVReadingOptions
    
    /// The set of strings that stores acceptable spellings for empty values.
    ///
    /// Defaults to `["", "#N/A", "#N/A N/A", "#NA", "N/A", "NA", "NULL", "n/a", "null"]`.
    public var nilEncodings: Set<String> {
        get { csvReadingOptions.nilEncodings }
        set { csvReadingOptions.nilEncodings = newValue }
    }
    
    /// The set of strings that stores acceptable spellings for true Boolean values.
    ///
    /// Defaults to `["1", "True", "TRUE", "true"]`.
    public var trueEncodings: Set<String> {
        get { csvReadingOptions.trueEncodings }
        set { csvReadingOptions.trueEncodings = newValue }
    }
    
    /// The set of strings that stores acceptable spellings for false Boolean values.
    ///
    /// Defaults to `["0", "False", "FALSE", "false"]`.
    public var falseEncodings: Set<String> {
        get { csvReadingOptions.falseEncodings }
        set { csvReadingOptions.falseEncodings = newValue }
    }
    
    /// The type to use for floating-point numeric values.
    ///
    /// Defaults to ``CSVType/double``.
    public var floatingPointType: CSVType {
        get { csvReadingOptions.floatingPointType }
        set { csvReadingOptions.floatingPointType = newValue }
    }
    
    /// An array of closures that parse a date from a string.
    public var dateParsers: [(String) -> Date?] {
        get { csvReadingOptions.dateParsers }
        set { csvReadingOptions.dateParsers = newValue }
    }
    
    /// A Boolean value that indicates whether to ignore empty lines.
    ///
    /// Defaults to `true`.
    public var ignoresEmptyLines: Bool {
        get { csvReadingOptions.ignoresEmptyLines }
        set { csvReadingOptions.ignoresEmptyLines = newValue }
    }
    
    /// A Boolean value that indicates whether to enable quoting.
    ///
    /// When `true`, the contents of a quoted field can contain special characters, such as the field
    /// delimiter and newlines. Defaults to `true`.
    public var usesQuoting: Bool {
        get { csvReadingOptions.usesQuoting }
        set { csvReadingOptions.usesQuoting = newValue }
    }
    
    /// A Boolean value that indicates whether to enable escaping.
    ///
    /// When `true`, you can escape special characters, such as the field delimiter, by prefixing them with
    /// the escape character, which is the backslash (`\`) by default. Defaults to `false`.
    public var usesEscaping: Bool {
        get { csvReadingOptions.usesEscaping }
        set { csvReadingOptions.usesEscaping = newValue }
    }
    
    /// The character that separates data fields in a CSV file, typically a comma.
    ///
    /// Defaults to comma (`,`).
    public var delimiter: Character { csvReadingOptions.delimiter }
    
    /// The character that precedes other characters, such as quotation marks,
    /// so that the parser interprets them as literal characters instead of special ones.
    ///
    /// Defaults to backslash(`\`).
    public var escapeCharacter: Character { csvReadingOptions.escapeCharacter }
    
    public var dataParser: ((String) -> Data?)?
    
    public var decimalParser: ((String) -> Decimal?)?
    
    public var urlParser: ((String) -> URL?)?
    
    public var nilAsEmptyString: Bool = false
    
    public init(
        nilEncodings: Set<String>   = [ "", "#N/A", "#N/A N/A", "#NA", "N/A", "NA", "NULL", "n/a", "null" ],
        trueEncodings: Set<String>  = [ "1", "True", "TRUE", "true" ],
        falseEncodings: Set<String> = [ "0", "False", "FALSE", "false" ],
        floatingPointType: CSVType = .double,
        dateParsers: [(String) -> Date?] = [],
        ignoresEmptyLines: Bool = true,
        usesQuoting: Bool = true,
        usesEscaping: Bool = false,
        delimiter: Character = ",",
        escapeCharacter: Character = "\\",
        dataParser: ((String) -> Data?)? = nil,
        decimalParser: ((String) -> Decimal?)? = nil,
        urlParser: ((String) -> URL?)? = nil,
        nitAsEmptyString: Bool = false
    ) {
        csvReadingOptions = .init(
            hasHeaderRow: true,
            nilEncodings: nilEncodings,
            trueEncodings: trueEncodings,
            falseEncodings: falseEncodings,
            floatingPointType: .double,
            ignoresEmptyLines: ignoresEmptyLines,
            usesQuoting: usesQuoting,
            usesEscaping: usesEscaping,
            delimiter: delimiter,
            escapeCharacter: escapeCharacter
        )
        self.dataParser = dataParser
        self.decimalParser = decimalParser
        self.urlParser = urlParser
        self.nilAsEmptyString = nitAsEmptyString
    }
    
    public init() { csvReadingOptions = .init() }
    
    func hasHeaderRow(_ value: Bool) -> (readingOptions: Self, csvReadingOptions: CSVReadingOptions) {
        var options = self
        options.csvReadingOptions.hasHeaderRow = value
        return (readingOptions: options, csvReadingOptions: options.csvReadingOptions)
    }
    
    func parserForType<T>(_ type: T.Type) -> ((String) -> Any)? {
        switch type {
        case is Data.Type:
            return dataParser
        case is Decimal.Type:
            return decimalParser
        case is URL.Type:
            return urlParser
        default:
            return nil
        }
    }
}
