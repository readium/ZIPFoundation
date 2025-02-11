//
//  Copyright 2025 Readium Foundation. All rights reserved.
//  Use of this source code is governed by the BSD-style license
//  available in the top-level LICENSE file of the project.
//

import Foundation

public enum DataSourceError: Error {
    case notWritable
    case unexpectedDataLength
}

public protocol DataSource: Sendable {
    
    /// Gets the total length of the source, if known.
    func length() async throws -> UInt64
    
    /// Indicates whether the ``DataSource`` can be modified using `openWrite`.
    var isWritable: Bool { get }
    
    /// Opens a transaction to read data from the source.
    ///
    /// You **must** call `transaction.close()` when you are done.
    func openRead() async throws -> DataSourceTransaction
    
    /// Opens a transaction to modify the source.
    ///
    /// You **must** call `transaction.close()` when you are done.
    func openWrite() async throws -> WritableDataSourceTransaction
}

extension DataSource {
    
    public func openWrite() async throws -> WritableDataSourceTransaction {
        throw DataSourceError.notWritable
    }
}

/// A ``DataSource`` abstract the access to the ZIP data.
public protocol DataSourceTransaction: Sendable {

    /// Gets the current offset position.
    func position() async throws -> UInt64
    
    /// Moves to the given offset position.
    func seek(to position: UInt64) async throws
    
    /// Reads the requested `length` amount of data.
    func read(length: Int) async throws -> Data
}

public protocol WritableDataSourceTransaction: DataSourceTransaction {
    
    /// Writes the given `data` at the current position.
    func write(_ data: Data) async throws
    
    func writeLargeChunk(_ data: Data, size: UInt64, bufferSize: Int) async throws
    
    /// Truncates the data source to the given `length`.
    func truncate(to length: UInt64) async throws
    
    /// Commits any pending writing operations to the data source.
    func flush() async throws
}

extension DataSourceTransaction {
    
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
