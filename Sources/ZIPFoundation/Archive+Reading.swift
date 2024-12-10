//
//  Archive+Reading.swift
//  ZIPFoundation
//
//  Copyright © 2017-2024 Thomas Zoechling, https://www.peakstep.com and the ZIP Foundation project authors.
//  Released under the MIT License.
//
//  See https://github.com/weichsel/ZIPFoundation/blob/master/LICENSE for license information.
//

import Foundation

extension Archive {
    /// Read a ZIP `Entry` from the receiver and write it to `url`.
    ///
    /// - Parameters:
    ///   - entry: The ZIP `Entry` to read.
    ///   - url: The destination file URL.
    ///   - bufferSize: The maximum size of the read buffer and the decompression buffer (if needed).
    ///   - skipCRC32: Optional flag to skip calculation of the CRC32 checksum to improve performance.
    ///   - allowUncontainedSymlinks: Optional flag to allow symlinks that point to paths outside the destination.
    ///   - progress: A progress object that can be used to track or cancel the extract operation.
    /// - Returns: The checksum of the processed content or 0 if the `skipCRC32` flag was set to `true`.
    /// - Throws: An error if the destination file cannot be written or the entry contains malformed content.
    public func extract(_ entry: Entry, to url: URL, bufferSize: Int = defaultReadChunkSize,
                        skipCRC32: Bool = false, allowUncontainedSymlinks: Bool = false,
                        progress: Progress? = nil) throws -> CRC32 {
        guard bufferSize > 0 else {
            throw ArchiveError.invalidBufferSize
        }
        let fileManager = FileManager()
        var checksum = CRC32(0)
        switch entry.type {
        case .file:
            guard fileManager.itemExists(at: url) == false else {
                throw CocoaError(.fileWriteFileExists, userInfo: [NSFilePathErrorKey: url.path])
            }
            try fileManager.createParentDirectoryStructure(for: url)
            let destinationRepresentation = fileManager.fileSystemRepresentation(withPath: url.path)
            guard let destinationFile: FILEPointer = fopen(destinationRepresentation, "wb+") else {
                throw POSIXError(errno, path: url.path)
            }
            defer { fclose(destinationFile) }
            let consumer = { _ = try Data.write(chunk: $0, to: destinationFile) }
            checksum = try self.extract(entry, bufferSize: bufferSize, skipCRC32: skipCRC32,
                                        progress: progress, consumer: consumer)
        case .directory:
            let consumer = { (_: Data) in
                try fileManager.createDirectory(at: url, withIntermediateDirectories: true, attributes: nil)
            }
            checksum = try self.extract(entry, bufferSize: bufferSize, skipCRC32: skipCRC32,
                                        progress: progress, consumer: consumer)
        case .symlink:
            guard fileManager.itemExists(at: url) == false else {
                throw CocoaError(.fileWriteFileExists, userInfo: [NSFilePathErrorKey: url.path])
            }
            let consumer = { (data: Data) in
                guard let linkPath = String(data: data, encoding: .utf8) else { throw ArchiveError.invalidEntryPath }

                let parentURL = url.deletingLastPathComponent()
                let isAbsolutePath = (linkPath as NSString).isAbsolutePath
                let linkURL = URL(fileURLWithPath: linkPath, relativeTo: isAbsolutePath ? nil : parentURL)
                let isContained = allowUncontainedSymlinks || linkURL.isContained(in: parentURL)
                guard isContained else { throw ArchiveError.uncontainedSymlink }

                try fileManager.createParentDirectoryStructure(for: url)
                try fileManager.createSymbolicLink(atPath: url.path, withDestinationPath: linkPath)
            }
            checksum = try self.extract(entry, bufferSize: bufferSize, skipCRC32: skipCRC32,
                                        progress: progress, consumer: consumer)
        }
        try fileManager.transferAttributes(from: entry, toItemAtURL: url)
        return checksum
    }

    /// Read a ZIP `Entry` from the receiver and forward its contents to a `Consumer` closure.
    ///
    /// - Parameters:
    ///   - entry: The ZIP `Entry` to read.
    ///   - bufferSize: The maximum size of the read buffer and the decompression buffer (if needed).
    ///   - skipCRC32: Optional flag to skip calculation of the CRC32 checksum to improve performance.
    ///   - progress: A progress object that can be used to track or cancel the extract operation.
    ///   - consumer: A closure that consumes contents of `Entry` as `Data` chunks.
    /// - Returns: The checksum of the processed content or 0 if the `skipCRC32` flag was set to `true`..
    /// - Throws: An error if the destination file cannot be written or the entry contains malformed content.
    public func extract(_ entry: Entry, bufferSize: Int = defaultReadChunkSize, skipCRC32: Bool = false,
                        progress: Progress? = nil, consumer: Consumer) throws -> CRC32 {
        guard bufferSize > 0 else {
            throw ArchiveError.invalidBufferSize
        }
        var checksum = CRC32(0)
        let localFileHeader = entry.localFileHeader
        guard entry.dataOffset <= .max else { throw ArchiveError.invalidLocalHeaderDataOffset }
        fseeko(self.archiveFile, off_t(entry.dataOffset), SEEK_SET)
        progress?.totalUnitCount = self.totalUnitCountForReading(entry)
        switch entry.type {
        case .file:
            guard let compressionMethod = CompressionMethod(rawValue: localFileHeader.compressionMethod) else {
                throw ArchiveError.invalidCompressionMethod
            }
            switch compressionMethod {
            case .none: checksum = try self.readUncompressed(entry: entry, bufferSize: bufferSize,
                                                             skipCRC32: skipCRC32, progress: progress, with: consumer)
            case .deflate: checksum = try self.readCompressed(entry: entry, bufferSize: bufferSize,
                                                              skipCRC32: skipCRC32, progress: progress, with: consumer)
            }
        case .directory:
            try consumer(Data())
            progress?.completedUnitCount = self.totalUnitCountForReading(entry)
        case .symlink:
            let localFileHeader = entry.localFileHeader
            let size = Int(localFileHeader.compressedSize)
            let data = try Data.readChunk(of: size, from: self.archiveFile)
            checksum = data.crc32(checksum: 0)
            try consumer(data)
            progress?.completedUnitCount = self.totalUnitCountForReading(entry)
        }
        return checksum
    }
    
    /// Read a portion of a ZIP `Entry` from the receiver and forward its contents to a `Consumer` closure.
    ///
    /// - Parameters:
    ///   - range: The portion range in the (decompressed) entry.
    ///   - entry: The ZIP `Entry` to read.
    ///   - bufferSize: The maximum size of the read buffer and the decompression buffer (if needed).
    ///   - consumer: A closure that consumes contents of `Entry` as `Data` chunks.
    /// - Throws: An error if the destination file cannot be written or the entry contains malformed content.
    public func extractRange(
        _ range: Range<UInt64>,
        of entry: Entry,
        bufferSize: Int = defaultReadChunkSize,
        consumer: Consumer
    ) throws {
        guard entry.type == .file else {
            throw ArchiveError.entryIsNotAFile
        }
        guard bufferSize > 0 else {
            throw ArchiveError.invalidBufferSize
        }
        guard range.lowerBound >= 0, range.upperBound <= entry.uncompressedSize else {
            throw ArchiveError.rangeOutOfBounds
        }
        let localFileHeader = entry.localFileHeader
        guard entry.dataOffset <= .max else {
            throw ArchiveError.invalidLocalHeaderDataOffset
        }
        
        guard let compressionMethod = CompressionMethod(rawValue: localFileHeader.compressionMethod) else {
            throw ArchiveError.invalidCompressionMethod
        }
        
        switch compressionMethod {
        case .none:
            try extractStoredRange(range, of: entry, bufferSize: bufferSize, consumer: consumer)
            
        case .deflate:
            try extractCompressedRange(range, of: entry, bufferSize: bufferSize, consumer: consumer)
        }
    }
    
    /// Ranges of stored entries can be accessed directly, as the requested
    /// indices match the ones in the archive file.
    private func extractStoredRange(
        _ range: Range<UInt64>,
        of entry: Entry,
        bufferSize: Int,
        consumer: Consumer
    ) throws {
        fseeko(archiveFile, off_t(entry.dataOffset + range.lowerBound), SEEK_SET)
        
        _ = try Data.consumePart(
            of: Int64(range.count),
            chunkSize: bufferSize,
            skipCRC32: true,
            provider: { pos, chunkSize -> Data in
                try Data.readChunk(of: chunkSize, from: self.archiveFile)
            },
            consumer: consumer
        )
    }
    
    /// Ranges of deflated entries cannot be accessed randomly. We must read
    /// and inflate the entry from the start until we reach the requested range.
    private func extractCompressedRange(
        _ range: Range<UInt64>,
        of entry: Entry,
        bufferSize: Int,
        consumer: Consumer
    ) throws {
        var bytesRead: UInt64 = 0
        
        do {
            fseeko(archiveFile, off_t(entry.dataOffset), SEEK_SET)
            
            _ = try readCompressed(
                entry: entry,
                bufferSize: bufferSize,
                skipCRC32: true
            ) { chunk in
                let chunkSize = UInt64(chunk.count)
                
                if bytesRead >= range.lowerBound {
                    if bytesRead + chunkSize > range.upperBound {
                        let remainingBytes = range.upperBound - bytesRead
                        try consumer(chunk[..<remainingBytes])
                    } else {
                        try consumer(chunk)
                    }
                } else if bytesRead + chunkSize > range.lowerBound {
                    // Calculate the overlap and pass the relevant portion of the chunk
                    let start = range.lowerBound - bytesRead
                    let end = Swift.min(chunkSize, range.upperBound - bytesRead)
                    try consumer(chunk[start..<end])
                }
                
                bytesRead += chunkSize
                
                guard bytesRead < range.upperBound else {
                    throw EndOfRange()
                }
            }
        } catch is EndOfRange { }
    }
    
    private struct EndOfRange: Error {}
}
