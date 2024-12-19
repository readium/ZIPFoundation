//
//  DataSource.swift
//  ZIPFoundation
//
//  Created by Mickaël on 12/17/24.
//

import Foundation

/// A ``DataSource`` abstract the access to the ZIP data.
public protocol DataSource {
    
    /// Gets the total length of the source, if known.
    func length() async throws -> UInt64

    /// Gets the current offset position.
    func position() async throws -> UInt64
    
    /// Moves to the given offset position.
    func seek(to position: UInt64) async throws
    
    /// Reads the requested `length` amount of data.
    func read(length: Int) async throws -> Data
    
    /// Closes the underlying handles.
    func close() async throws
}

public protocol WritableDataSource: DataSource {
    
    /// Writes the given `data` at the current position.
    func write(_ data: Data) async throws
    
    func writeLargeChunk(_ data: Data, size: UInt64, bufferSize: Int) async throws
    
    /// Truncates the data source to the given `length`.
    func truncate(to length: UInt64) async throws
    
    /// Commits any pending writing operations to the data source.
    func flush() async throws
}

public enum DataSourceError: Error {
    case unexpectedDataLength
}

extension DataSource {
    
    /// Reads a single int from the data.
    func readInt() async throws -> UInt32 {
        let data = try await read(length: 4)
        guard data.count == 4 else {
            throw DataSourceError.unexpectedDataLength
        }
        
        return data.withUnsafeBytes { rawBuffer in
            rawBuffer.load(as: UInt32.self)
        }
    }

    /// Reads a full serializable structure from the data.
    func readStruct<T>(at position: UInt64) async throws -> T? where T : DataSerializable {
        try await seek(to: position)
        
        return await T(
            data: try await read(length: T.size),
            additionalDataProvider: { additionalDataSize -> Data in
                try await read(length: additionalDataSize)
            }
        )
    }
}
