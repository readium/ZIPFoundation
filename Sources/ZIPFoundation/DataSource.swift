//
//  DataSource.swift
//  ZIPFoundation
//
//  Created by Mickaël on 12/17/24.
//

import Foundation

/// A ``DataSource`` abstract the access to the ZIP data.
protocol DataSource {
    
    /// Gets the total length of the source, if known.
    func length() throws -> UInt64

    /// Gets the current offset position.
    func position() throws -> UInt64
    
    /// Moves to the given offset position.
    func seek(to position: UInt64) throws
    
    /// Reads the requested `length` amount of data.
    func read(length: Int) throws -> Data
    
    /// Reads a single int from the data.
    func readInt() throws -> UInt32
    
    /// Reads a full serializable structure from the data.
    func readStruct<T : DataSerializable>(at position: UInt64) throws -> T?
    
    /// Closes the underlying handles.
    func close() throws
}

protocol WritableDataSource: DataSource {
    
    /// Writes the given `data` at the current position.
    func write(_ data: Data) throws
    
    func writeLargeChunk(_ data: Data, size: UInt64, bufferSize: Int) throws
    
    /// Truncates the data source to the given `length`.
    func truncate(to length: UInt64) throws
    
    /// Commits any pending writing operations to the data source.
    func flush() throws
}
