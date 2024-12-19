//
//  ReadingOptions.swift
//  SweepMap
//
//  Created by Doug on 12/11/24.
//  Copyright © 2024 Doug. All rights reserved.
//

import Foundation
import TabularData

public struct ReadingOptions {
    public var tabularOptions: CSVReadingOptions
    
    /// A Boolean value that indicates whether the CSV file has a header row.
    ///
    /// Defaults to `true`.
    public var hasHeaderRow: Bool {
        get { tabularOptions.hasHeaderRow }
        set { tabularOptions.hasHeaderRow = newValue }
    }

    /// The set of strings that stores acceptable spellings for empty values.
    ///
    /// Defaults to `["", "#N/A", "#N/A N/A", "#NA", "N/A", "NA", "NULL", "n/a", "null"]`.
    public var nilEncodings: Set<String> {
        get { tabularOptions.nilEncodings }
        set { tabularOptions.nilEncodings = newValue }
    }

    /// The set of strings that stores acceptable spellings for true Boolean values.
    ///
    /// Defaults to `["1", "True", "TRUE", "true"]`.
    public var trueEncodings: Set<String> {
        get { tabularOptions.trueEncodings }
        set { tabularOptions.trueEncodings = newValue }
    }

    /// The set of strings that stores acceptable spellings for false Boolean values.
    ///
    /// Defaults to `["0", "False", "FALSE", "false"]`.
    public var falseEncodings: Set<String> {
        get { tabularOptions.falseEncodings }
        set { tabularOptions.falseEncodings = newValue }
    }

    /// The type to use for floating-point numeric values.
    ///
    /// Defaults to ``CSVType/double``.
    public var floatingPointType: CSVType {
        get { tabularOptions.floatingPointType }
        set { tabularOptions.floatingPointType = newValue }
    }

    /// An array of closures that parse a date from a string.
    public var dateParsers: [(String) -> Date?] {
        get { tabularOptions.dateParsers }
        set { tabularOptions.dateParsers = newValue }
    }

    /// A Boolean value that indicates whether to ignore empty lines.
    ///
    /// Defaults to `true`.
    public var ignoresEmptyLines: Bool {
        get { tabularOptions.ignoresEmptyLines }
        set { tabularOptions.ignoresEmptyLines = newValue }
    }

    /// A Boolean value that indicates whether to enable quoting.
    ///
    /// When `true`, the contents of a quoted field can contain special characters, such as the field
    /// delimiter and newlines. Defaults to `true`.
    public var usesQuoting: Bool {
        get { tabularOptions.usesQuoting }
        set { tabularOptions.usesQuoting = newValue }
    }

    /// A Boolean value that indicates whether to enable escaping.
    ///
    /// When `true`, you can escape special characters, such as the field delimiter, by prefixing them with
    /// the escape character, which is the backslash (`\`) by default. Defaults to `false`.
    public var usesEscaping: Bool {
        get { tabularOptions.usesEscaping }
        set { tabularOptions.usesEscaping = newValue }
    }

    /// The character that separates data fields in a CSV file, typically a comma.
    ///
    /// Defaults to comma (`,`).
    public var delimiter: Character { tabularOptions.delimiter }

    /// The character that precedes other characters, such as quotation marks,
    /// so that the parser interprets them as literal characters instead of special ones.
    ///
    /// Defaults to backslash(`\`).
    public var escapeCharacter: Character { tabularOptions.escapeCharacter }

    public var dataParser: ((String) -> Data?)?
    
    public var decimalParser: ((String) -> Decimal?)?
    
    public var urlParser: ((String) -> URL?)?
    
    public init(
        hasHeaderRow: Bool = true,
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
        urlParser: ((String) -> URL?)? = nil
    ) {
        tabularOptions = .init(
            hasHeaderRow: hasHeaderRow,
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
    }

    public init() { tabularOptions = .init() }
    
    func decode<T: Decodable>(_ type: T.Type, forKey key: CodingKey?, rowNumber: Int, decoding: DataDecoder) throws -> T {
        if let parser = parserForType(type) {
            let string = try decoding.nextString(forKey: key)
            return parser(string) as! T
        } else {
            return try T(from: decoding)
        }
    }

    func decodeIfPresent<T: Decodable>(_ type: T.Type, rowNumber: Int, decoding: DataDecoder) throws -> T? {
        if let parser = parserForType(type) {
            guard let string = decoding.nextStringIfPresent(), !string.isEmpty else { return nil }
            return parser(string) as? T
        } else {
            return try T(from: decoding)
        }
    }

    private func parserForType<T>(_ type: T.Type) -> ((String) -> Any)? {
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
