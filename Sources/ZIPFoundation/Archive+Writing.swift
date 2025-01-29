//
//  Archive+Writing.swift
//  ZIPFoundation
//
//  Copyright © 2017-2024 Thomas Zoechling, https://www.peakstep.com and the ZIP Foundation project authors.
//  Released under the MIT License.
//
//  See https://github.com/weichsel/ZIPFoundation/blob/master/LICENSE for license information.
//

import Foundation

extension Archive {
    enum ModifyOperation: Int {
        case remove = -1
        case add = 1
    }

    typealias EndOfCentralDirectoryStructure = (EndOfCentralDirectoryRecord, ZIP64EndOfCentralDirectory?)

    /// Write files, directories or symlinks to the receiver.
    ///
    /// - Parameters:
    ///   - path: The path that is used to identify an `Entry` within the `Archive` file.
    ///   - baseURL: The base URL of the resource to add.
    ///              The `baseURL` combined with `path` must form a fully qualified file URL.
    ///   - compressionMethod: Indicates the `CompressionMethod` that should be applied to `Entry`.
    ///                        By default, no compression will be applied.
    ///   - bufferSize: The maximum size of the write buffer and the compression buffer (if needed).
    ///   - progress: A progress object that can be used to track or cancel the add operation.
    /// - Throws: An error if the source file cannot be read or the receiver is not writable.
    public func addEntry(with path: String, relativeTo baseURL: URL,
                         compressionMethod: CompressionMethod = .none,
                         bufferSize: Int = defaultWriteChunkSize, progress: Progress? = nil) async throws {
        let fileURL = baseURL.appendingPathComponent(path)

        try await self.addEntry(with: path, fileURL: fileURL, compressionMethod: compressionMethod,
                          bufferSize: bufferSize, progress: progress)
    }

    /// Write files, directories or symlinks to the receiver.
    ///
    /// - Parameters:
    ///   - path: The path that is used to identify an `Entry` within the `Archive` file.
    ///   - fileURL: An absolute file URL referring to the resource to add.
    ///   - compressionMethod: Indicates the `CompressionMethod` that should be applied to `Entry`.
    ///                        By default, no compression will be applied.
    ///   - bufferSize: The maximum size of the write buffer and the compression buffer (if needed).
    ///   - progress: A progress object that can be used to track or cancel the add operation.
    /// - Throws: An error if the source file cannot be read or the receiver is not writable.
    public func addEntry(with path: String, fileURL: URL, compressionMethod: CompressionMethod = .none,
                         bufferSize: Int = defaultWriteChunkSize, progress: Progress? = nil) async throws {
        guard let url = self.url else { throw ArchiveError.unwritableArchive }
        let fileManager = FileManager()
        guard fileManager.itemExists(at: fileURL) else {
            throw CocoaError(.fileReadNoSuchFile, userInfo: [NSFilePathErrorKey: fileURL.path])
        }
        let type = try FileManager.typeForItem(at: fileURL)
        // symlinks do not need to be readable
        guard type == .symlink || fileManager.isReadableFile(atPath: fileURL.path) else {
            throw CocoaError(.fileReadNoPermission, userInfo: [NSFilePathErrorKey: fileURL.path])
        }
        let modDate = try FileManager.fileModificationDateTimeForItem(at: fileURL)
        let uncompressedSize = type == .directory ? 0 : try FileManager.fileSizeForItem(at: fileURL)
        let permissions = try FileManager.permissionsForItem(at: fileURL)
        var provider: Provider
        switch type {
        case .file:
            let entryFileSystemRepresentation = fileManager.fileSystemRepresentation(withPath: fileURL.path)
            guard let entryFile: FILEPointer = fopen(entryFileSystemRepresentation, "rb") else {
                throw POSIXError(errno, path: url.path)
            }
            defer { fclose(entryFile) }
            provider = { _, _ in return try Data.readChunk(of: bufferSize, from: entryFile) }
            try await self.addEntry(with: path, type: type, uncompressedSize: uncompressedSize,
                              modificationDate: modDate, permissions: permissions,
                              compressionMethod: compressionMethod, bufferSize: bufferSize,
                              progress: progress, provider: provider)
        case .directory:
            provider = { _, _ in return Data() }
            try await self.addEntry(with: path.hasSuffix("/") ? path : path + "/",
                              type: type, uncompressedSize: uncompressedSize,
                              modificationDate: modDate, permissions: permissions,
                              compressionMethod: compressionMethod, bufferSize: bufferSize,
                              progress: progress, provider: provider)
        case .symlink:
            provider = { _, _ -> Data in
                let linkDestination = try fileManager.destinationOfSymbolicLink(atPath: fileURL.path)
                let linkFileSystemRepresentation = fileManager.fileSystemRepresentation(withPath: linkDestination)
                let linkLength = Int(strlen(linkFileSystemRepresentation))
                let linkBuffer = UnsafeBufferPointer(start: linkFileSystemRepresentation, count: linkLength)
                return Data(buffer: linkBuffer)
            }
            try await self.addEntry(with: path, type: type, uncompressedSize: uncompressedSize,
                              modificationDate: modDate, permissions: permissions,
                              compressionMethod: compressionMethod, bufferSize: bufferSize,
                              progress: progress, provider: provider)
        }
    }

    /// Write files, directories or symlinks to the receiver.
    ///
    /// - Parameters:
    ///   - path: The path that is used to identify an `Entry` within the `Archive` file.
    ///   - type: Indicates the `Entry.EntryType` of the added content.
    ///   - uncompressedSize: The uncompressed size of the data that is going to be added with `provider`.
    ///   - modificationDate: A `Date` describing the file modification date of the `Entry`.
    ///                       Default is the current `Date`.
    ///   - permissions: POSIX file permissions for the `Entry`.
    ///                  Default is `0`o`644` for files and symlinks and `0`o`755` for directories.
    ///   - compressionMethod: Indicates the `CompressionMethod` that should be applied to `Entry`.
    ///                        By default, no compression will be applied.
    ///   - bufferSize: The maximum size of the write buffer and the compression buffer (if needed).
    ///   - progress: A progress object that can be used to track or cancel the add operation.
    ///   - provider: A closure that accepts a position and a chunk size. Returns a `Data` chunk.
    /// - Throws: An error if the source data is invalid or the receiver is not writable.
    public func addEntry(with path: String, type: Entry.EntryType, uncompressedSize: Int64,
                         modificationDate: Date = Date(), permissions: UInt16? = nil,
                         compressionMethod: CompressionMethod = .none, bufferSize: Int = defaultWriteChunkSize,
                         progress: Progress? = nil, provider: Provider) async throws {
        guard self.accessMode != .read, let dataSource = dataSource as? WritableDataSource else { throw ArchiveError.unwritableArchive }
        // Directories and symlinks cannot be compressed
        let compressionMethod = type == .file ? compressionMethod : .none
        progress?.totalUnitCount = type == .directory ? defaultDirectoryUnitCount : uncompressedSize
        let (eocdRecord, zip64EOCD) = (self.endOfCentralDirectoryRecord, self.zip64EndOfCentralDirectory)
        guard self.offsetToStartOfCentralDirectory <= .max else { throw ArchiveError.invalidCentralDirectoryOffset }
        var startOfCD = self.offsetToStartOfCentralDirectory
        try await dataSource.seek(to: startOfCD)
        let existingSize = self.sizeOfCentralDirectory
        let existingData = try await dataSource.read(length: Int(existingSize))
        try await dataSource.seek(to: startOfCD)
        let fileHeaderStart = try await dataSource.position()
        let modDateTime = modificationDate.fileModificationDateTime
        
        do {
            // Local File Header
            var localFileHeader = try await self.writeLocalFileHeader(path: path, compressionMethod: compressionMethod,
                                                                size: (UInt64(uncompressedSize), 0), checksum: 0,
                                                                modificationDateTime: modDateTime)
            // File Data
            let (written, checksum) = try await self.writeEntry(uncompressedSize: uncompressedSize, type: type,
                                                          compressionMethod: compressionMethod, bufferSize: bufferSize,
                                                          progress: progress, provider: provider)
            startOfCD = try await dataSource.position()
            // Write the local file header a second time. Now with compressedSize (if applicable) and a valid checksum.
            try await dataSource.seek(to: fileHeaderStart)
            localFileHeader = try await self.writeLocalFileHeader(path: path, compressionMethod: compressionMethod,
                                                            size: (UInt64(uncompressedSize), UInt64(written)),
                                                            checksum: checksum, modificationDateTime: modDateTime)
            // Central Directory
            try await dataSource.seek(to: startOfCD)
            try await dataSource.writeLargeChunk(existingData, size: existingSize, bufferSize: bufferSize)
            let permissions = permissions ?? (type == .directory ? defaultDirectoryPermissions : defaultFilePermissions)
            let externalAttributes = FileManager.externalFileAttributesForEntry(of: type, permissions: permissions)
            let centralDir = try await self.writeCentralDirectoryStructure(localFileHeader: localFileHeader,
                                                                     relativeOffset: UInt64(fileHeaderStart),
                                                                     externalFileAttributes: externalAttributes)
            // End of Central Directory Record (including ZIP64 End of Central Directory Record/Locator)
            let startOfEOCD = try await dataSource.position()
            let eocd = try await self.writeEndOfCentralDirectory(centralDirectoryStructure: centralDir,
                                                           startOfCentralDirectory: UInt64(startOfCD),
                                                           startOfEndOfCentralDirectory: startOfEOCD, operation: .add)
            (self.endOfCentralDirectoryRecord, self.zip64EndOfCentralDirectory) = eocd
            
            try await dataSource.flush()
            
        } catch ArchiveError.cancelledOperation {
            try await rollback(UInt64(fileHeaderStart), (existingData, existingSize), bufferSize, eocdRecord, zip64EOCD)
            throw ArchiveError.cancelledOperation
        }
    }

    /// Remove a ZIP `Entry` from the receiver.
    ///
    /// - Parameters:
    ///   - entry: The `Entry` to remove.
    ///   - bufferSize: The maximum size for the read and write buffers used during removal.
    ///   - progress: A progress object that can be used to track or cancel the remove operation.
    /// - Throws: An error if the `Entry` is malformed or the receiver is not writable.
    public func remove(_ entry: Entry, bufferSize: Int = defaultReadChunkSize, progress: Progress? = nil) async throws {
        guard self.accessMode != .read, let dataSource = dataSource as? WritableDataSource else { throw ArchiveError.unwritableArchive }
        let (tempArchive, tempDir) = try await self.makeTempArchive()
        defer { tempDir.map { try? FileManager().removeItem(at: $0) } }
        progress?.totalUnitCount = self.totalUnitCountForRemoving(entry)
        var centralDirectoryData = Data()
        var offset: UInt64 = 0
        for try await currentEntry in self {
            let cds = currentEntry.centralDirectoryStructure
            if currentEntry != entry {
                let entryStart = cds.effectiveRelativeOffsetOfLocalHeader
                try await dataSource.seek(to: entryStart)
                let provider: Provider = { (_, chunkSize) -> Data in
                    try await dataSource.read(length: chunkSize)
                }
                let consumer: Consumer = {
                    if progress?.isCancelled == true { throw ArchiveError.cancelledOperation }
                    try await tempArchive.writableDataSource.write($0)
                    progress?.completedUnitCount += Int64($0.count)
                }
                guard currentEntry.localSize <= .max else { throw ArchiveError.invalidLocalHeaderSize }
                _ = try await Data.consumePart(of: Int64(currentEntry.localSize), chunkSize: bufferSize,
                                         provider: provider, consumer: consumer)
                let updatedCentralDirectory = updateOffsetInCentralDirectory(centralDirectoryStructure: cds,
                                                                             updatedOffset: entryStart - offset)
                centralDirectoryData.append(updatedCentralDirectory.data)
            } else { offset = currentEntry.localSize }
        }
        let startOfCentralDirectory = try await tempArchive.dataSource.position()
        try await tempArchive.writableDataSource.write(centralDirectoryData)
        let startOfEndOfCentralDirectory = try await tempArchive.dataSource.position()
        tempArchive.endOfCentralDirectoryRecord = self.endOfCentralDirectoryRecord
        tempArchive.zip64EndOfCentralDirectory = self.zip64EndOfCentralDirectory
        let ecodStructure = try await tempArchive.writeEndOfCentralDirectory(
            centralDirectoryStructure: entry.centralDirectoryStructure,
            startOfCentralDirectory: startOfCentralDirectory,
            startOfEndOfCentralDirectory: startOfEndOfCentralDirectory,
            operation: .remove
        )
        (tempArchive.endOfCentralDirectoryRecord, tempArchive.zip64EndOfCentralDirectory) = ecodStructure
        (self.endOfCentralDirectoryRecord, self.zip64EndOfCentralDirectory) = ecodStructure
        try await tempArchive.writableDataSource.flush()
        try await self.replaceCurrentArchive(with: tempArchive)
    }

    func replaceCurrentArchive(with archive: Archive) async throws {
        guard let url = self.url, let archiveURL = archive.url else { throw ArchiveError.unwritableArchive }
        
        try dataSource.close()
        
        let fileManager = FileManager()
#if os(macOS) || os(iOS) || os(tvOS) || os(visionOS) || os(watchOS)
        do {
            _ = try fileManager.replaceItemAt(url, withItemAt: archiveURL)
        } catch {
            _ = try fileManager.removeItem(at: url)
            _ = try fileManager.moveItem(at: archiveURL, to: url)
        }
#else
        _ = try fileManager.removeItem(at: url)
        _ = try fileManager.moveItem(at: archiveURL, to: url)
#endif
        self.dataSource = try await FileDataSource(url: url, mode: .write)
    }
}

// MARK: - Private

private extension Archive {

    func updateOffsetInCentralDirectory(centralDirectoryStructure: CentralDirectoryStructure,
                                        updatedOffset: UInt64) -> CentralDirectoryStructure {
        let zip64ExtendedInformation = Entry.ZIP64ExtendedInformation(
            zip64ExtendedInformation: centralDirectoryStructure.zip64ExtendedInformation, offset: updatedOffset)
        let offsetInCD = updatedOffset < maxOffsetOfLocalFileHeader ? UInt32(updatedOffset) : UInt32.max
        return CentralDirectoryStructure(centralDirectoryStructure: centralDirectoryStructure,
                                         zip64ExtendedInformation: zip64ExtendedInformation,
                                         relativeOffset: offsetInCD)
    }

    func rollback(_ localFileHeaderStart: UInt64, _ existingCentralDirectory: (data: Data, size: UInt64),
                  _ bufferSize: Int, _ endOfCentralDirRecord: EndOfCentralDirectoryRecord,
                  _ zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory?) async throws {
        try await writableDataSource.flush()
        try await writableDataSource.truncate(to: localFileHeaderStart)
        try await writableDataSource.seek(to: localFileHeaderStart)
        try await writableDataSource.writeLargeChunk(existingCentralDirectory.data, size: existingCentralDirectory.size,
                                     bufferSize: bufferSize)
        try await writableDataSource.write(existingCentralDirectory.data)
        if let zip64EOCD = zip64EndOfCentralDirectory {
            try await writableDataSource.write(zip64EOCD.data)
        }
        try await writableDataSource.write(endOfCentralDirRecord.data)
        try await writableDataSource.flush()
    }

    func makeTempArchive() async throws -> (Archive, URL?) {
        var archive: Archive
        var url: URL?
        let manager = FileManager()
        let tempDir = URL.temporaryReplacementDirectoryURL(for: self)
        let uniqueString = ProcessInfo.processInfo.globallyUniqueString
        let tempArchiveURL = tempDir.appendingPathComponent(uniqueString)
        try manager.createParentDirectoryStructure(for: tempArchiveURL)
        let tempArchive = try await Archive(url: tempArchiveURL, accessMode: .create)
        archive = tempArchive
        url = tempDir
        return (archive, url)
    }
}
