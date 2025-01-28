//
//  OrderedSet.swift
//  TabularCSV
//
//  Created by Doug on 1/18/25.
//

public struct OrderedSet<T: Hashable & Sendable>: ExpressibleByArrayLiteral, Sendable {
    fileprivate var objects: [T] = []
    fileprivate var indexOfKey: [T: Int] = [:]
    
    public init(arrayLiteral objects: T...) {
        self.objects = objects
        indexOfKey = objects.enumerated().reduce(into: [:]) { $0[$1.element] = $1.offset }
    }
    
    // O(1)
    public mutating func add(_ object: T) {
        guard indexOfKey[object] == nil else {
            return
        }
        
        objects.append(object)
        indexOfKey[object] = objects.count - 1
    }
    
    // O(n)
    public mutating func insert(_ object: T, at index: Int) {
        assert(index < objects.count, "Index should be smaller than object count")
        assert(index >= 0, "Index should be bigger than 0")
        
        guard indexOfKey[object] == nil else {
            return
        }
        
        objects.insert(object, at: index)
        indexOfKey[object] = index
        for i in index+1..<objects.count {
            indexOfKey[objects[i]] = i
        }
    }
    
    // O(1)
    public func object(at index: Int) -> T {
        assert(index < objects.count, "Index should be smaller than object count")
        assert(index >= 0, "Index should be bigger than 0")
        
        return objects[index]
    }
    
    // O(1)
    public func contains(_ object: T) -> Bool {
        indexOfKey[object] != nil
    }
    
    // O(1)
    public mutating func set(_ object: T, at index: Int) {
        assert(index < objects.count, "Index should be smaller than object count")
        assert(index >= 0, "Index should be bigger than 0")
        
        guard indexOfKey[object] == nil else {
            return
        }
        
        indexOfKey.removeValue(forKey: objects[index])
        indexOfKey[object] = index
        objects[index] = object
    }
    
    // O(1)
    public func indexOf(_ object: T) -> Int {
        return indexOfKey[object] ?? -1
    }
    
    // O(n)
    public mutating func remove(_ object: T) {
        guard let index = indexOfKey[object] else {
            return
        }
        
        indexOfKey.removeValue(forKey: object)
        objects.remove(at: index)
        for i in index..<objects.count {
            indexOfKey[objects[i]] = i
        }
    }
    
    public mutating func removeAll() {
        objects.removeAll()
        indexOfKey.removeAll()
    }
    
    public subscript(index: Int) -> T {
        object(at: index)
    }
    
    public func all() -> [T] {
        return objects
    }
}
