//
//  Archive.swift
//  ZIPFoundation
//
//  Copyright © 2017-2024 Thomas Zoechling, https://www.peakstep.com and the ZIP Foundation project authors.
//  Released under the MIT License.
//
//  See https://github.com/weichsel/ZIPFoundation/blob/master/LICENSE for license information.
//

import Foundation

/// The default chunk size when reading entry data from an archive.
public let defaultReadChunkSize = Int(16*1024)
/// The default chunk size when writing entry data to an archive.
public let defaultWriteChunkSize = defaultReadChunkSize
/// The default permissions for newly added entries.
public let defaultFilePermissions = UInt16(0o644)
/// The default permissions for newly added directories.
public let defaultDirectoryPermissions = UInt16(0o755)
let defaultPOSIXBufferSize = defaultReadChunkSize
let defaultDirectoryUnitCount = Int64(1)
let minEndOfCentralDirectoryOffset = UInt64(22)
let endOfCentralDirectoryStructSignature = 0x06054b50
let localFileHeaderStructSignature = 0x04034b50
let dataDescriptorStructSignature = 0x08074b50
let centralDirectoryStructSignature = 0x02014b50

/// A sequence of uncompressed or compressed ZIP entries.
///
/// You use an `Archive` to create, read or update ZIP files.
/// To read an existing ZIP file, you have to pass in an existing file `URL` and `AccessMode.read`:
///
///     var archiveURL = URL(fileURLWithPath: "/path/file.zip")
///     var archive = Archive(url: archiveURL, accessMode: .read)
///
/// An `Archive` is a sequence of entries. You can
/// iterate over an archive using a `for`-`in` loop to get access to individual `Entry` objects:
///
///     for entry in archive {
///         print(entry.path)
///     }
///
/// Each `Entry` in an `Archive` is represented by its `path`. You can
/// use `path` to retrieve the corresponding `Entry` from an `Archive` via subscripting:
///
///     let entry = archive['/path/file.txt']
///
/// To create a new `Archive`, pass in a non-existing file URL and `AccessMode.create`. To modify an
/// existing `Archive` use `AccessMode.update`:
///
///     var archiveURL = URL(fileURLWithPath: "/path/file.zip")
///     var archive = Archive(url: archiveURL, accessMode: .update)
///     try archive?.addEntry("test.txt", relativeTo: baseURL, compressionMethod: .deflate)
public actor Archive {

    typealias LocalFileHeader = Entry.LocalFileHeader
    typealias DataDescriptor = Entry.DefaultDataDescriptor
    typealias ZIP64DataDescriptor = Entry.ZIP64DataDescriptor
    typealias CentralDirectoryStructure = Entry.CentralDirectoryStructure

    /// An error that occurs during reading, creating or updating a ZIP file.
    public enum ArchiveError: Error {
        /// Thrown when an archive file is either damaged or inaccessible.
        case unreadableArchive
        /// Thrown when an archive is either opened with AccessMode.read or the destination file is unwritable.
        case unwritableArchive
        /// Thrown when the path of an `Entry` cannot be stored in an archive.
        case invalidEntryPath
        /// Thrown when an `Entry` can't be stored in the archive with the proposed compression method.
        case invalidCompressionMethod
        /// Thrown when the stored checksum of an `Entry` doesn't match the checksum during reading.
        case invalidCRC32
        /// Thrown when an extract, add or remove operation was canceled.
        case cancelledOperation
        /// Thrown when an extract operation was called with zero or negative `bufferSize` parameter.
        case invalidBufferSize
        /// Thrown when uncompressedSize/compressedSize exceeds `Int64.max` (Imposed by file API).
        case invalidEntrySize
        /// Thrown when the local header cannot be found.
        case localHeaderNotFound
        /// Thrown when the offset of local header data exceeds `Int64.max` (Imposed by file API).
        case invalidLocalHeaderDataOffset
        /// Thrown when the size of local header exceeds `Int64.max` (Imposed by file API).
        case invalidLocalHeaderSize
        /// Thrown when the offset of central directory exceeds `Int64.max` (Imposed by file API).
        case invalidCentralDirectoryOffset
        /// Thrown when the size of central directory exceeds `UInt64.max` (Imposed by ZIP specification).
        case invalidCentralDirectorySize
        /// Thrown when number of entries in central directory exceeds `UInt64.max` (Imposed by ZIP specification).
        case invalidCentralDirectoryEntryCount
        /// Thrown when an archive does not contain the required End of Central Directory Record.
        case missingEndOfCentralDirectoryRecord
        /// Thrown when an entry contains a symlink pointing to a path outside the destination directory.
        case uncontainedSymlink
        /// Thrown when the requested range is out of bounds for the entry.
        case rangeOutOfBounds
        /// The requested entry is not a file but a directory or symlink.
        case entryIsNotAFile
    }

    /// The access mode for an `Archive`.
    public enum AccessMode: UInt, Sendable {
        /// Indicates that a newly instantiated `Archive` should create its backing file.
        case create
        /// Indicates that a newly instantiated `Archive` should read from an existing backing file.
        case read
        /// Indicates that a newly instantiated `Archive` should update an existing backing file.
        case update
    }

    /// The version of an `Archive`
    enum Version: UInt16 {
        /// The minimum version for deflate compressed archives
        case v20 = 20
        /// The minimum version for archives making use of ZIP64 extensions
        case v45 = 45
    }

    struct EndOfCentralDirectoryRecord: DataSerializable {
        let endOfCentralDirectorySignature = UInt32(endOfCentralDirectoryStructSignature)
        let numberOfDisk: UInt16
        let numberOfDiskStart: UInt16
        let totalNumberOfEntriesOnDisk: UInt16
        let totalNumberOfEntriesInCentralDirectory: UInt16
        let sizeOfCentralDirectory: UInt32
        let offsetToStartOfCentralDirectory: UInt32
        let zipFileCommentLength: UInt16
        let zipFileCommentData: Data
        static let size = 22
    }

    /// URL of an Archive's backing file.
    public nonisolated let url: URL?
    /// Access mode for an archive file.
    public nonisolated let accessMode: AccessMode
    
    nonisolated let readChunkSize: Int
    
    var dataSource: DataSource
    private(set) var endOfCentralDirectoryRecord: EndOfCentralDirectoryRecord
    private(set) var zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory?
    private var pathEncoding: String.Encoding?
    
    var endOfCentralDirectory: EndOfCentralDirectoryStructure {
        (endOfCentralDirectoryRecord, zip64EndOfCentralDirectory)
    }
    
    func setEndOfCentralDirectory(_ structure: EndOfCentralDirectoryStructure) {
        endOfCentralDirectoryRecord = structure.0
        zip64EndOfCentralDirectory = structure.1
    }
    
    func setZIP64EndOfCentralDirectory(_ directory: ZIP64EndOfCentralDirectory?) {
        zip64EndOfCentralDirectory = directory
    }

    var totalNumberOfEntriesInCentralDirectory: UInt64 {
        zip64EndOfCentralDirectory?.record.totalNumberOfEntriesInCentralDirectory
        ?? UInt64(endOfCentralDirectoryRecord.totalNumberOfEntriesInCentralDirectory)
    }
    var sizeOfCentralDirectory: UInt64 {
        zip64EndOfCentralDirectory?.record.sizeOfCentralDirectory
        ?? UInt64(endOfCentralDirectoryRecord.sizeOfCentralDirectory)
    }
    var offsetToStartOfCentralDirectory: UInt64 {
        zip64EndOfCentralDirectory?.record.offsetToStartOfCentralDirectory
        ?? UInt64(endOfCentralDirectoryRecord.offsetToStartOfCentralDirectory)
    }

    /// Initializes a new ZIP `Archive`.
    ///
    /// You can use this initalizer to create new archive files or to read and update existing ones.
    /// The `mode` parameter indicates the intended usage of the archive: `.read`, `.create` or `.update`.
    /// - Parameters:
    ///   - url: File URL to the receivers backing file.
    ///   - mode: Access mode of the receiver.
    ///   - pathEncoding: Encoding for entry paths. Overrides the encoding specified in the archive.
    ///                   This encoding is only used when _decoding_ paths from the receiver.
    ///                   Paths of entries added with `addEntry` are always UTF-8 encoded.
    /// - Returns: An archive initialized with a backing file at the passed in file URL and the given access mode
    ///   or `nil` if the following criteria are not met:
    /// - Note:
    ///   - The file URL _must_ point to an existing file for `AccessMode.read`.
    ///   - The file URL _must_ point to a non-existing file for `AccessMode.create`.
    ///   - The file URL _must_ point to an existing file for `AccessMode.update`.
    public init(url: URL, accessMode mode: AccessMode, defaultReadChunkSize: Int = defaultReadChunkSize, pathEncoding: String.Encoding? = nil) async throws {
        self.url = url
        self.accessMode = mode
        self.pathEncoding = pathEncoding
        let config = try await Archive.makeBackingConfiguration(for: url, mode: mode)
        self.dataSource = config.dataSource
        self.readChunkSize = defaultReadChunkSize
        self.endOfCentralDirectoryRecord = config.endOfCentralDirectoryRecord
        self.zip64EndOfCentralDirectory = config.zip64EndOfCentralDirectory
    }

    public init(url: URL?, dataSource: DataSource, defaultReadChunkSize: Int = defaultReadChunkSize, pathEncoding: String.Encoding? = nil) async throws {
        self.url = url
        self.accessMode = .read
        self.pathEncoding = pathEncoding
        let config = try await Archive.makeBackingConfiguration(for: dataSource)
        self.dataSource = config.dataSource
        self.readChunkSize = defaultReadChunkSize
        self.endOfCentralDirectoryRecord = config.endOfCentralDirectoryRecord
        self.zip64EndOfCentralDirectory = config.zip64EndOfCentralDirectory
    }

    private var entriesTask: Task<[Entry], Error>?
    
    /// Returns the list of entries in the archive.
    public func entries() async throws -> [Entry] {
        if entriesTask == nil {
            entriesTask = Task { try await readEntries() }
        }
        
        return try await entriesTask!.value
    }
    
    private func readEntries() async throws -> [Entry] {
        guard totalNumberOfEntriesInCentralDirectory > 0 else {
            return []
        }
        
        var entries: [Entry] = []
        var directoryIndex = offsetToStartOfCentralDirectory
        let transaction = try await dataSource.openRead()
        
        for _ in 0..<totalNumberOfEntriesInCentralDirectory {
            guard let centralDirStruct: CentralDirectoryStructure = try await transaction.readStruct(at: directoryIndex) else {
                continue
            }
            
            if let entry = Entry(centralDirectoryStructure: centralDirStruct) {
                entries.append(entry)
            }
            
            directoryIndex += UInt64(CentralDirectoryStructure.size)
            directoryIndex += UInt64(centralDirStruct.fileNameLength)
            directoryIndex += UInt64(centralDirStruct.extraFieldLength)
            directoryIndex += UInt64(centralDirStruct.fileCommentLength)
        }
        
        return entries
    }

    /// Retrieve the ZIP `Entry` with the given `path` from the receiver.
    ///
    /// - Note: The ZIP file format specification does not enforce unique paths for entries.
    ///   Therefore an archive can contain multiple entries with the same path. This method
    ///   always returns the first `Entry` with the given `path`.
    ///
    /// - Parameter path: A relative file path identifying the corresponding `Entry`.
    /// - Returns: An `Entry` with the given `path`. Otherwise, `nil`.
    public func get(_ path: String) async throws -> Entry? {
        let result = try await entries().first {
            if let encoding = self.pathEncoding {
                return $0.path(using: encoding) == path
            } else {
                return $0.path == path
            }
        }
        return result
    }
    
    /// Cache for local file headers.
    private var localFileHeaders: [String: Task<LocalFileHeader, Error>] = [:]
    
    /// Retrieves the local file header for the given `entry`.
    func localFileHeader(for entry: Entry) async throws -> LocalFileHeader {
        if let task = localFileHeaders[entry.path] {
            return try await task.value
        }
        
        let task = Task {
            let transaction = try await dataSource.openRead()
            let centralDirStruct = entry.centralDirectoryStructure
            let offset = UInt64(centralDirStruct.effectiveRelativeOffsetOfLocalHeader)
            guard
                var localFileHeader: LocalFileHeader = try await transaction.readStruct(at: offset)
            else {
                throw Archive.ArchiveError.localHeaderNotFound
            }
            
            /// We only load the data descriptors if we are in writing mode, because
            /// they might need to be written over. In read mode it is is
            /// superfluous as the same infos are in the central directory structure.
            if centralDirStruct.usesDataDescriptor && accessMode != .read {
                let additionalSize = UInt64(localFileHeader.fileNameLength) + UInt64(localFileHeader.extraFieldLength)
                let isCompressed = centralDirStruct.compressionMethod != CompressionMethod.none.rawValue
                let dataSize = isCompressed
                ? centralDirStruct.effectiveCompressedSize
                : centralDirStruct.effectiveUncompressedSize
                let descriptorPosition = offset + UInt64(LocalFileHeader.size) + additionalSize + dataSize
                if centralDirStruct.isZIP64 {
                    localFileHeader.zip64DataDescriptor = try await transaction.readStruct(at: descriptorPosition)
                } else {
                    localFileHeader.dataDescriptor = try await transaction.readStruct(at: descriptorPosition)
                }
            }
            
            return localFileHeader
        }
        localFileHeaders[entry.path] = task
        
        return try await task.value
    }
    
    /// Called when the archive was modified.
    func didWrite() {
        // Clears the caches.
        entriesTask = nil
        localFileHeaders = [:]
    }

    // MARK: - Helpers

    static func scanForEndOfCentralDirectoryRecord(in dataSource: DataSource)
    async throws -> EndOfCentralDirectoryStructure? {
        let transaction = try await dataSource.openRead()
        var eocdOffset: UInt64 = 0
        var index = minEndOfCentralDirectoryOffset
        let archiveLength = try await dataSource.length()
        while eocdOffset == 0 && index <= archiveLength {
            try await transaction.seek(to: archiveLength - index)
            let potentialDirectoryEndTag = try await transaction.readInt()
            if potentialDirectoryEndTag == UInt32(endOfCentralDirectoryStructSignature) {
                eocdOffset = UInt64(archiveLength - index)
                guard let eocd: EndOfCentralDirectoryRecord = try await transaction.readStruct(at: eocdOffset) else {
                    return nil
                }
                let zip64EOCD = try await scanForZIP64EndOfCentralDirectory(in: dataSource, eocdOffset: eocdOffset)
                return (eocd, zip64EOCD)
            }
            index += 1
        }
        return nil
    }

    private static func scanForZIP64EndOfCentralDirectory(in dataSource: DataSource, eocdOffset: UInt64)
    async throws -> ZIP64EndOfCentralDirectory? {
        guard UInt64(ZIP64EndOfCentralDirectoryLocator.size) < eocdOffset else {
            return nil
        }
        let locatorOffset = eocdOffset - UInt64(ZIP64EndOfCentralDirectoryLocator.size)
        
        guard UInt64(ZIP64EndOfCentralDirectoryRecord.size) < locatorOffset else {
            return nil
        }
        
        let transaction = try await dataSource.openRead()
        let recordOffset = locatorOffset - UInt64(ZIP64EndOfCentralDirectoryRecord.size)
        guard let locator: ZIP64EndOfCentralDirectoryLocator = try await transaction.readStruct(at: locatorOffset),
              let record: ZIP64EndOfCentralDirectoryRecord = try await transaction.readStruct(at: recordOffset) else {
            return nil
        }
        return ZIP64EndOfCentralDirectory(record: record, locator: locator)
    }
}

extension Archive.EndOfCentralDirectoryRecord {

    var data: Data {
        var endOfCDSignature = self.endOfCentralDirectorySignature
        var numberOfDisk = self.numberOfDisk
        var numberOfDiskStart = self.numberOfDiskStart
        var totalNumberOfEntriesOnDisk = self.totalNumberOfEntriesOnDisk
        var totalNumberOfEntriesInCD = self.totalNumberOfEntriesInCentralDirectory
        var sizeOfCentralDirectory = self.sizeOfCentralDirectory
        var offsetToStartOfCD = self.offsetToStartOfCentralDirectory
        var zipFileCommentLength = self.zipFileCommentLength
        var data = Data()
        withUnsafePointer(to: &endOfCDSignature, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &numberOfDisk, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &numberOfDiskStart, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &totalNumberOfEntriesOnDisk, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &totalNumberOfEntriesInCD, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &sizeOfCentralDirectory, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &offsetToStartOfCD, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        withUnsafePointer(to: &zipFileCommentLength, { data.append(UnsafeBufferPointer(start: $0, count: 1))})
        data.append(self.zipFileCommentData)
        return data
    }

    init?(data: Data, additionalDataProvider provider: (Int) async throws -> Data) async {
        guard data.count == Archive.EndOfCentralDirectoryRecord.size else { return nil }
        guard data.scanValue(start: 0) == endOfCentralDirectorySignature else { return nil }
        self.numberOfDisk = data.scanValue(start: 4)
        self.numberOfDiskStart = data.scanValue(start: 6)
        self.totalNumberOfEntriesOnDisk = data.scanValue(start: 8)
        self.totalNumberOfEntriesInCentralDirectory = data.scanValue(start: 10)
        self.sizeOfCentralDirectory = data.scanValue(start: 12)
        self.offsetToStartOfCentralDirectory = data.scanValue(start: 16)
        self.zipFileCommentLength = data.scanValue(start: 20)
        guard let commentData = try? await provider(Int(self.zipFileCommentLength)) else { return nil }
        guard commentData.count == Int(self.zipFileCommentLength) else { return nil }
        self.zipFileCommentData = commentData
    }

    init(record: Archive.EndOfCentralDirectoryRecord,
         numberOfEntriesOnDisk: UInt16,
         numberOfEntriesInCentralDirectory: UInt16,
         updatedSizeOfCentralDirectory: UInt32,
         startOfCentralDirectory: UInt32) {
        self.numberOfDisk = record.numberOfDisk
        self.numberOfDiskStart = record.numberOfDiskStart
        self.totalNumberOfEntriesOnDisk = numberOfEntriesOnDisk
        self.totalNumberOfEntriesInCentralDirectory = numberOfEntriesInCentralDirectory
        self.sizeOfCentralDirectory = updatedSizeOfCentralDirectory
        self.offsetToStartOfCentralDirectory = startOfCentralDirectory
        self.zipFileCommentLength = record.zipFileCommentLength
        self.zipFileCommentData = record.zipFileCommentData
    }
}
