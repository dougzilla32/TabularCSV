//
//  OrderedDictionary.swift
//  TabularCSV
//
//  Created by Doug on 1/23/25.
//

public struct OrderedDictionary<Key: Hashable & Sendable, Value> {
    private var keys: OrderedSet<Key> = []
    private var values: [Key: Value] = [:]
    
    public init() { }
    
    public init<S>(uniqueKeysWithValues keysAndValues: S) where S : Sequence, S.Element == (Key, Value) {
        for (key, value) in keysAndValues {
            self[key] = value
        }
    }

    public subscript(key: Key) -> Value? {
        get {
            return values[key]
        }
        set(newValue) {
            if let value = newValue {
                keys.add(key)
                values[key] = value
            } else {
                values[key] = nil
                keys.remove(key)
            }
        }
    }

    public func value(at index: Int) -> Value? {
        guard index >= 0 && index < count else { return nil }
        return values[keys[index]]
    }

    public func key(at index: Int) -> Key? {
        guard index >= 0 && index < count else { return nil }
        return keys[index]
    }

    public mutating func remove(forKey key: Key) {
        values[key] = nil
        keys.remove(key)
    }

    public var count: Int {
        keys.all().count
    }

    public var orderedKeys: [Key] {
        keys.all()
    }
}
