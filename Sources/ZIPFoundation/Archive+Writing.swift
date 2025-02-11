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
        let fileManager = FileManager.default
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
            provider = { @Sendable _, _ -> Data in
                let fileManager = FileManager.default
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
        guard
            accessMode != .read,
            dataSource.isWritable
        else {
            throw ArchiveError.unwritableArchive
        }
        
        let transaction = try await dataSource.openWrite()
        // Directories and symlinks cannot be compressed
        let compressionMethod = type == .file ? compressionMethod : .none
        progress?.totalUnitCount = type == .directory ? defaultDirectoryUnitCount : uncompressedSize
        let (eocdRecord, zip64EOCD) = (self.endOfCentralDirectoryRecord, self.zip64EndOfCentralDirectory)
        guard self.offsetToStartOfCentralDirectory <= .max else { throw ArchiveError.invalidCentralDirectoryOffset }
        var startOfCD = self.offsetToStartOfCentralDirectory
        try await transaction.seek(to: startOfCD)
        let existingSize = self.sizeOfCentralDirectory
        let existingData = try await transaction.read(length: Int(existingSize))
        try await transaction.seek(to: startOfCD)
        let fileHeaderStart = try await transaction.position()
        let modDateTime = modificationDate.fileModificationDateTime
        
        do {
            // Local File Header
            var localFileHeader = try await self.writeLocalFileHeader(
                transaction: transaction,
                path: path, compressionMethod: compressionMethod,
                size: (UInt64(uncompressedSize), 0), checksum: 0,
                modificationDateTime: modDateTime
            )
            // File Data
            let (written, checksum) = try await self.writeEntry(
                transaction: transaction,
                uncompressedSize: uncompressedSize, type: type,
                compressionMethod: compressionMethod, bufferSize: bufferSize,
                progress: progress, provider: provider
            )
            startOfCD = try await transaction.position()
            // Write the local file header a second time. Now with compressedSize (if applicable) and a valid checksum.
            try await transaction.seek(to: fileHeaderStart)
            localFileHeader = try await self.writeLocalFileHeader(
                transaction: transaction,
                path: path, compressionMethod: compressionMethod,
                size: (UInt64(uncompressedSize), UInt64(written)),
                checksum: checksum, modificationDateTime: modDateTime
            )
            // Central Directory
            try await transaction.seek(to: startOfCD)
            try await transaction.writeLargeChunk(existingData, size: existingSize, bufferSize: bufferSize)
            let permissions = permissions ?? (type == .directory ? defaultDirectoryPermissions : defaultFilePermissions)
            let externalAttributes = FileManager.externalFileAttributesForEntry(of: type, permissions: permissions)
            let centralDir = try await self.writeCentralDirectoryStructure(
                transaction: transaction,
                localFileHeader: localFileHeader,
                relativeOffset: UInt64(fileHeaderStart),
                externalFileAttributes: externalAttributes
            )
            // End of Central Directory Record (including ZIP64 End of Central Directory Record/Locator)
            let startOfEOCD = try await transaction.position()
            let eocd = try await self.writeEndOfCentralDirectory(
                transaction: transaction,
                centralDirectoryStructure: centralDir,
                startOfCentralDirectory: UInt64(startOfCD),
                startOfEndOfCentralDirectory: startOfEOCD, operation: .add
            )
            self.setEndOfCentralDirectory(eocd)
            
            try await transaction.flush()
            
        } catch ArchiveError.cancelledOperation {
            try await rollback(transaction, UInt64(fileHeaderStart), (existingData, existingSize), bufferSize, eocdRecord, zip64EOCD)
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
        guard
            accessMode != .read,
            dataSource.isWritable
        else {
            throw ArchiveError.unwritableArchive
        }
        
        let transaction = try await dataSource.openWrite()
        let (tempArchive, tempDir) = try await self.makeTempArchive()
        let tempTransaction = try await tempArchive.dataSource.openWrite()
        defer { tempDir.map { try? FileManager().removeItem(at: $0) } }
        progress?.totalUnitCount = self.totalUnitCountForRemoving(entry)
        var centralDirectoryData = Data()
        var offset: UInt64 = 0
        for try await currentEntry in self {
            let cds = currentEntry.centralDirectoryStructure
            if currentEntry != entry {
                let entryStart = cds.effectiveRelativeOffsetOfLocalHeader
                try await transaction.seek(to: entryStart)
                let provider: Provider = { (_, chunkSize) -> Data in
                    try await transaction.read(length: chunkSize)
                }
                let consumer: Consumer = { data in
                    if progress?.isCancelled == true { throw ArchiveError.cancelledOperation }
                    try await tempTransaction.write(data)
                    progress?.completedUnitCount += Int64(data.count)
                }
                guard currentEntry.localSize <= .max else { throw ArchiveError.invalidLocalHeaderSize }
                _ = try await Data.consumePart(of: Int64(currentEntry.localSize), chunkSize: bufferSize,
                                               provider: provider, consumer: consumer)
                let updatedCentralDirectory = updateOffsetInCentralDirectory(centralDirectoryStructure: cds,
                                                                             updatedOffset: entryStart - offset)
                centralDirectoryData.append(updatedCentralDirectory.data)
            } else { offset = currentEntry.localSize }
        }
        
        let startOfCentralDirectory = try await tempTransaction.position()
        try await tempTransaction.write(centralDirectoryData)
        let startOfEndOfCentralDirectory = try await tempTransaction.position()
        await tempArchive.setEndOfCentralDirectory(self.endOfCentralDirectory)
        let ecodStructure = try await tempArchive.writeEndOfCentralDirectory(
            transaction: tempTransaction,
            centralDirectoryStructure: entry.centralDirectoryStructure,
            startOfCentralDirectory: startOfCentralDirectory,
            startOfEndOfCentralDirectory: startOfEndOfCentralDirectory,
            operation: .remove
        )
        await tempArchive.setEndOfCentralDirectory(ecodStructure)
        self.setEndOfCentralDirectory(ecodStructure)
        try await tempTransaction.flush()
        try await self.replaceCurrentArchive(with: tempArchive)
    }

    func replaceCurrentArchive(with archive: Archive) async throws {
        guard let url = self.url, let archiveURL = archive.url else { throw ArchiveError.unwritableArchive }
        
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
        self.dataSource = try await FileDataSource(url: url, isWritable: true)
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

    func rollback(
        _ transaction: WritableDataSourceTransaction,
        _ localFileHeaderStart: UInt64,
        _ existingCentralDirectory: (data: Data, size: UInt64),
        _ bufferSize: Int,
        _ endOfCentralDirRecord: EndOfCentralDirectoryRecord,
        _ zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory?
    ) async throws {
        try await transaction.flush()
        try await transaction.truncate(to: localFileHeaderStart)
        try await transaction.seek(to: localFileHeaderStart)
        try await transaction.writeLargeChunk(
            existingCentralDirectory.data,
            size: existingCentralDirectory.size,
            bufferSize: bufferSize
        )
        try await transaction.write(existingCentralDirectory.data)
        if let zip64EOCD = zip64EndOfCentralDirectory {
            try await transaction.write(zip64EOCD.data)
        }
        try await transaction.write(endOfCentralDirRecord.data)
        try await transaction.flush()
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
