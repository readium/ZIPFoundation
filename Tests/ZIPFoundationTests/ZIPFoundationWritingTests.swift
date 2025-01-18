//
//  ZIPFoundationWritingTests.swift
//  ZIPFoundation
//
//  Copyright © 2017-2024 Thomas Zoechling, https://www.peakstep.com and the ZIP Foundation project authors.
//  Released under the MIT License.
//
//  See https://github.com/weichsel/ZIPFoundation/blob/master/LICENSE for license information.
//

import XCTest
@testable import ReadiumZIPFoundation

extension ZIPFoundationTests {

    func testCreateArchiveAddUncompressedEntry() async {
        let archive = await self.archive(for: #function, mode: .create)
        let assetURL = self.resourceURL(for: #function, pathExtension: "png")
        do {
            let relativePath = assetURL.lastPathComponent
            let baseURL = assetURL.deletingLastPathComponent()
            try await archive.addEntry(with: relativePath, relativeTo: baseURL)
        } catch {
            XCTFail("Failed to add uncompressed entry archive with error : \(error)")
        }
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddCompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        let assetURL = self.resourceURL(for: #function, pathExtension: "png")
        do {
            let relativePath = assetURL.lastPathComponent
            let baseURL = assetURL.deletingLastPathComponent()
            try await archive.addEntry(with: relativePath, relativeTo: baseURL, compressionMethod: .deflate)
        } catch {
            XCTFail("Failed to add compressed entry folder archive : \(error)")
        }
        let entry = try await archive.get(assetURL.lastPathComponent)
        XCTAssertNotNil(entry)
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddDirectory() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        do {
            try await archive.addEntry(with: "Test", type: .directory,
                                 uncompressedSize: Int64(0), provider: { _, _ in return Data()})
        } catch {
            XCTFail("Failed to add directory entry without file system representation to archive.")
        }
        let testEntry = try await archive.get("Test")
        XCTAssertNotNil(testEntry)
        let uniqueString = ProcessInfo.processInfo.globallyUniqueString
        let tempDirectoryURL = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(uniqueString)
        do {
            let fileManager = FileManager()
            try fileManager.createDirectory(at: tempDirectoryURL, withIntermediateDirectories: true, attributes: nil)
            let relativePath = tempDirectoryURL.lastPathComponent
            let baseURL = tempDirectoryURL.deletingLastPathComponent()
            try await archive.addEntry(with: relativePath + "/", relativeTo: baseURL)
        } catch {
            XCTFail("Failed to add directory entry to archive.")
        }
        let entry = try await archive.get(tempDirectoryURL.lastPathComponent + "/")
        XCTAssertNotNil(entry)
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddSymbolicLink() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        let rootDirectoryURL = ZIPFoundationTests.tempZipDirectoryURL.appendingPathComponent("SymbolicLinkDirectory")
        let symbolicLinkURL = rootDirectoryURL.appendingPathComponent("test.link")
        let assetURL = self.resourceURL(for: #function, pathExtension: "png")
        let fileManager = FileManager()
        do {
            try fileManager.createDirectory(at: rootDirectoryURL, withIntermediateDirectories: true, attributes: nil)
            try fileManager.createSymbolicLink(atPath: symbolicLinkURL.path, withDestinationPath: assetURL.path)
            let relativePath = symbolicLinkURL.lastPathComponent
            let baseURL = symbolicLinkURL.deletingLastPathComponent()
            try await archive.addEntry(with: relativePath, relativeTo: baseURL)
        } catch {
            XCTFail("Failed to add symbolic link to archive")
        }
        let entry = try await archive.get(symbolicLinkURL.lastPathComponent)
        XCTAssertNotNil(entry)
        await archive.checkIntegrity()
        do {
            try await archive.addEntry(with: "link", type: .symlink, uncompressedSize: Int64(10),
                                 provider: { (_, count) -> Data in
                return Data(count: count)
            })
        } catch {
            XCTFail("Failed to add symbolic link to archive")
        }
        let entry2 = try await archive.get("link")
        XCTAssertNotNil(entry2)
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddEntryErrorConditions() async {
        let archive = await self.archive(for: #function, mode: .create)
        let tempPath = NSTemporaryDirectory()
        var nonExistantURL = URL(fileURLWithPath: tempPath)
        nonExistantURL.appendPathComponent("invalid.path")
        let nonExistantRelativePath = nonExistantURL.lastPathComponent
        let nonExistantBaseURL = nonExistantURL.deletingLastPathComponent()
        await XCTAssertCocoaError(try await archive.addEntry(with: nonExistantRelativePath, relativeTo: nonExistantBaseURL),
                            throwsErrorWithCode: .fileReadNoSuchFile)
        // Cover the error code path when `fopen` fails during entry addition.
        let assetURL = self.resourceURL(for: #function, pathExtension: "txt")
        let entryAddition = {
            let relativePath = assetURL.lastPathComponent
            let baseURL = assetURL.deletingLastPathComponent()
            await self.XCTAssertPOSIXError(try await archive.addEntry(with: relativePath, relativeTo: baseURL),
                                     throwsErrorWithCode: .EMFILE)
        }
        await self.runWithFileDescriptorLimit(0) {
            try? await entryAddition()
        }
    }

    func testArchiveAddEntryErrorConditions() async {
        let readonlyArchive = await self.archive(for: #function, mode: .read)
        await XCTAssertSwiftError(try await readonlyArchive.addEntry(with: "Test",
                                                         type: .directory,
                                                         uncompressedSize: Int64(0),
                                                         provider: { _, _ in return Data() }),
                            throws: Archive.ArchiveError.unwritableArchive)
    }

    func testCreateArchiveAddZeroSizeUncompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        let assetURL = self.resourceURL(for: #function, pathExtension: "txt")
        do {
            let relativePath = assetURL.lastPathComponent
            let baseURL = assetURL.deletingLastPathComponent()
            try await archive.addEntry(with: relativePath, relativeTo: baseURL)
        } catch {
            XCTFail("Failed to add zero-size uncompressed entry to archive with error : \(error)")
        }
        let entry = try await archive.get(assetURL.lastPathComponent)
        XCTAssertNotNil(entry)
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddZeroSizeCompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        let assetURL = self.resourceURL(for: #function, pathExtension: "txt")
        do {
            let relativePath = assetURL.lastPathComponent
            let baseURL = assetURL.deletingLastPathComponent()
            try await archive.addEntry(with: relativePath, relativeTo: baseURL, compressionMethod: .deflate)
        } catch {
            XCTFail("Failed to add zero-size compressed entry to archive with error : \(error)")
        }
        let entry = try await archive.get(assetURL.lastPathComponent)
        XCTAssertNotNil(entry)
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddLargeUncompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        let size = 1024*1024*20
        let data = Data.makeRandomData(size: size)
        let entryName = ProcessInfo.processInfo.globallyUniqueString
        do {
            try await archive.addEntry(with: entryName, type: .file,
                                 uncompressedSize: Int64(size), provider: { (position, bufferSize) -> Data in
                let upperBound = Swift.min(size, Int(position) + bufferSize)
                let range = Range(uncheckedBounds: (lower: Int(position), upper: upperBound))
                return data.subdata(in: range)
            })
        } catch {
            XCTFail("Failed to add large entry to uncompressed archive with error : \(error)")
        }
        guard let entry = try await archive.get(entryName) else {
            XCTFail("Failed to add large entry to uncompressed archive")
            return
        }
        XCTAssert(entry.checksum == data.crc32(checksum: 0))
        await archive.checkIntegrity()
    }

    func testCreateArchiveAddLargeCompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        let size = 1024*1024*20
        let data = Data.makeRandomData(size: size)
        let entryName = ProcessInfo.processInfo.globallyUniqueString
        do {
            try await archive.addEntry(with: entryName, type: .file, uncompressedSize: Int64(size),
                                 compressionMethod: .deflate,
                                 provider: { (position, bufferSize) -> Data in
                let upperBound = Swift.min(size, Int(position) + bufferSize)
                let range = Range(uncheckedBounds: (lower: Int(position), upper: upperBound))
                return data.subdata(in: range)
            })
        } catch {
            XCTFail("Failed to add large entry to compressed archive with error : \(error)")
        }
        guard let entry = try await archive.get(entryName) else {
            XCTFail("Failed to add large entry to compressed archive")
            return
        }
        let dataCRC32 = data.crc32(checksum: 0)
        XCTAssert(entry.checksum == dataCRC32)
        await archive.checkIntegrity()
    }

    func testRemoveUncompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .update)
        guard let entryToRemove = try await archive.get("test/data.random") else {
            XCTFail("Failed to find entry to remove in uncompressed folder"); return
        }
        do {
            try await archive.remove(entryToRemove)
        } catch {
            XCTFail("Failed to remove entry from uncompressed folder archive with error : \(error)")
        }
        await archive.checkIntegrity()
    }

    func testRemoveCompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .update)
        guard let entryToRemove = try await archive.get("test/data.random") else {
            XCTFail("Failed to find entry to remove in compressed folder archive"); return
        }
        do {
            try await archive.remove(entryToRemove)
        } catch {
            XCTFail("Failed to remove entry from compressed folder archive with error : \(error)")
        }
        await archive.checkIntegrity()
    }

    func testRemoveDataDescriptorCompressedEntry() async throws {
        let archive = await self.archive(for: #function, mode: .update)
        guard let entryToRemove = try await archive.get("second.txt") else {
            XCTFail("Failed to find entry to remove in compressed folder archive")
            return
        }
        do {
            try await archive.remove(entryToRemove)
        } catch {
            XCTFail("Failed to remove entry to compressed folder archive with error : \(error)")
        }
        await archive.checkIntegrity()
    }

    func testRemoveEntryErrorConditions() async throws {
        let archive = await self.archive(for: #function, mode: .update)
        guard let entryToRemove = try await archive.get("test/data.random") else {
            XCTFail("Failed to find entry to remove in uncompressed folder")
            return
        }
        // We don't have access to the temp archive file that Archive.remove
        // uses. To exercise the error code path, we temporarily limit the number of open files for
        // the test process to exercise the error code path here.
        await XCTAssertNoThrowAsync(try await self.runWithFileDescriptorLimit(0) {
            await XCTAssertCocoaError(try await archive.remove(entryToRemove), throwsErrorWithCode: .fileWriteUnknown)
        })
        let readonlyArchive = await self.archive(for: #function, mode: .read)
        await XCTAssertSwiftError(try await readonlyArchive.remove(entryToRemove), throws: Archive.ArchiveError.unwritableArchive)
    }

    func testArchiveCreateErrorConditions() async {
        let existantURL = ZIPFoundationTests.tempZipDirectoryURL
        await XCTAssertCocoaError(try await Archive(url: existantURL, accessMode: .create),
                            throwsErrorWithCode: .fileWriteFileExists)
        let processInfo = ProcessInfo.processInfo
        var noEndOfCentralDirectoryArchiveURL = ZIPFoundationTests.tempZipDirectoryURL
        noEndOfCentralDirectoryArchiveURL.appendPathComponent(processInfo.globallyUniqueString)
        let fullPermissionAttributes = [FileAttributeKey.posixPermissions: NSNumber(value: defaultFilePermissions)]
        let fileManager = FileManager()
        let result = fileManager.createFile(atPath: noEndOfCentralDirectoryArchiveURL.path, contents: nil,
                                            attributes: fullPermissionAttributes)
        XCTAssert(result == true)
        await XCTAssertSwiftError(try await Archive(url: noEndOfCentralDirectoryArchiveURL, accessMode: .update),
                            throws: Archive.ArchiveError.missingEndOfCentralDirectoryRecord)
    }

    func testArchiveUpdateErrorConditions() async throws {
        try await self.runWithUnprivilegedGroup {
            var nonUpdatableArchiveURL = ZIPFoundationTests.tempZipDirectoryURL
            let processInfo = ProcessInfo.processInfo
            nonUpdatableArchiveURL.appendPathComponent(processInfo.globallyUniqueString)
            let noPermissionAttributes = [FileAttributeKey.posixPermissions: NSNumber(value: Int16(0o000))]
            let fileManager = FileManager()
            let result = fileManager.createFile(atPath: nonUpdatableArchiveURL.path, contents: nil,
                                                attributes: noPermissionAttributes)
            XCTAssert(result == true)
            await XCTAssertPOSIXError(try await Archive(url: nonUpdatableArchiveURL, accessMode: .update),
                                throwsErrorWithCode: .EACCES)
        }
    }

    func testReplaceCurrentArchiveWithArchiveCrossLink() {
#if os(macOS)
        let createVolumeExpectation = expectation(description: "Creation of temporary additional volume")
        let unmountVolumeExpectation = expectation(description: "Unmount temporary additional volume")
        let tempDir = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString)
        do {
            try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true, attributes: nil)
            let volName = "Test_\(UUID().uuidString)"
            let task = try NSUserScriptTask.makeVolumeCreationTask(at: tempDir, volumeName: volName)
            task.execute { (error) in
                guard error == nil else {
                    XCTFail("\(String(describing: error))")
                    return
                }
                let vol2URL = URL(fileURLWithPath: "/Volumes/\(volName)")
                defer {
                    let options: FileManager.UnmountOptions = [.allPartitionsAndEjectDisk, .withoutUI]
                    FileManager.default.unmountVolume(at: vol2URL, options:
                                                        options, completionHandler: { (error) in
                        guard error == nil else {
                            XCTFail("\(String(describing: error))")
                            return
                        }
                        unmountVolumeExpectation.fulfill()
                    })
                }
                let vol1ArchiveURL = tempDir.appendingPathComponent("vol1Archive")
                let vol2ArchiveURL = vol2URL.appendingPathComponent("vol2Archive")
                do {
                    let vol1Archive = try Archive(url: vol1ArchiveURL, accessMode: .create)
                    let vol2Archive = try Archive(url: vol2ArchiveURL, accessMode: .create)
                    try vol1Archive.replaceCurrentArchive(with: vol2Archive)
                } catch {
                    type(of: self).tearDown()
                    XCTFail("\(String(describing: error))")
                    return
                }
                createVolumeExpectation.fulfill()
            }
        } catch {
            XCTFail("\(error)")
            return
        }
        defer { try? FileManager.default.removeItem(at: tempDir) }

        waitForExpectations(timeout: 30.0)
#endif
    }
}
