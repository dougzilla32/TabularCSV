//
//  CSVPrimitive.swift
//  TabularCSV
//
//  Created by Doug on 1/4/25.
//

import TabularData

public protocol CSVPrimitive: Codable, LosslessStringConvertible {
    static var csvType: CSVType { get }
}

extension Bool:   CSVPrimitive { public static var csvType: CSVType { .boolean } }
extension String: CSVPrimitive { public static var csvType: CSVType { .string  } }
extension Double: CSVPrimitive { public static var csvType: CSVType { .double  } }
extension Float:  CSVPrimitive { public static var csvType: CSVType { .float   } }
extension FixedWidthInteger where Self: CSVPrimitive { public static var csvType: CSVType { .integer } }

extension Int:    CSVPrimitive {}
extension Int8:   CSVPrimitive {}
extension Int16:  CSVPrimitive {}
extension Int32:  CSVPrimitive {}
extension Int64:  CSVPrimitive {}
extension UInt:   CSVPrimitive {}
extension UInt8:  CSVPrimitive {}
extension UInt16: CSVPrimitive {}
extension UInt32: CSVPrimitive {}
extension UInt64: CSVPrimitive {}

@available(macOS 15.0, iOS 18.0, watchOS 11.0, tvOS 18.0, visionOS 2.0, *)
extension UInt128: CSVPrimitive {}
