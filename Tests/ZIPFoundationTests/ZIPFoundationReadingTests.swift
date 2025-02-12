//
//  ZIPFoundationReadingTests.swift
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

    func testExtractUncompressedFolderEntries() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        for entry in try await archive.entries() {
            do {
                // Test extracting to memory
                var checksum = try await archive.extract(entry, bufferSize: 32, consumer: { _ in })
                XCTAssert(entry.checksum == checksum)
                // Test extracting to file
                var fileURL = self.createDirectory(for: #function)
                fileURL.appendPathComponent(entry.path)
                checksum = try await archive.extract(entry, to: fileURL)
                XCTAssert(entry.checksum == checksum)
                let fileManager = FileManager()
                XCTAssertTrue(fileManager.itemExists(at: fileURL))
                if entry.type == .file {
                    let fileData = try Data(contentsOf: fileURL)
                    let checksum = fileData.crc32(checksum: 0)
                    XCTAssert(checksum == entry.checksum)
                }
            } catch {
                XCTFail("Failed to unzip uncompressed folder entries")
            }
        }
    }

    func testExtractCompressedFolderEntries() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        for entry in try await archive.entries() {
            do {
                // Test extracting to memory
                var checksum = try await archive.extract(entry, bufferSize: 128, consumer: { _ in })
                XCTAssert(entry.checksum == checksum)
                // Test extracting to file
                var fileURL = self.createDirectory(for: #function)
                fileURL.appendPathComponent(entry.path)
                checksum = try await archive.extract(entry, to: fileURL)
                XCTAssert(entry.checksum == checksum)
                let fileManager = FileManager()
                XCTAssertTrue(fileManager.itemExists(at: fileURL))
                if entry.type != .directory {
                    let fileData = try Data(contentsOf: fileURL)
                    let checksum = fileData.crc32(checksum: 0)
                    XCTAssert(checksum == entry.checksum)
                }
            } catch {
                XCTFail("Failed to unzip compressed folder entries")
            }
        }
    }

    func testExtractUncompressedDataDescriptorArchive() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        for entry in try await archive.entries() {
            do {
                let checksum = try await archive.extract(entry, consumer: { _ in })
                XCTAssert(entry.checksum == checksum)
            } catch {
                XCTFail("Failed to unzip data descriptor archive")
            }
        }
    }

    func testExtractCompressedDataDescriptorArchive() async throws {
        let archive = await self.archive(for: #function, mode: .update)
        for entry in try await archive.entries() {
            do {
                let checksum = try await archive.extract(entry, consumer: { _ in })
                let lfh = try await archive.localFileHeader(for: entry)
                XCTAssert(lfh.checksum == checksum)
            } catch {
                XCTFail("Failed to unzip data descriptor archive")
            }
        }
    }

    func testExtractPreferredEncoding() async throws {
        let encoding = String.Encoding.utf8
        let archive = await self.archive(for: #function, mode: .read, preferredEncoding: encoding)
        await archive.checkIntegrity()
        let imageEntry = try await archive.get("data/pic👨‍👩‍👧‍👦🎂.jpg")
        XCTAssertNotNil(imageEntry)
        let textEntry = try await archive.get("data/Benoît.txt")
        XCTAssertNotNil(textEntry)
    }

    func testExtractMSDOSArchive() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        for entry in try await archive.entries() {
            do {
                let checksum = try await archive.extract(entry, consumer: { _ in })
                XCTAssert(entry.checksum == checksum)
            } catch {
                XCTFail("Failed to unzip MSDOS archive")
            }
        }
    }

    func testExtractErrorConditions() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        XCTAssertNotNil(archive)
        guard let fileEntry = try await archive.get("testZipItem.png") else {
            XCTFail("Failed to obtain test asset from archive.")
            return
        }
        XCTAssertNotNil(fileEntry)
        await XCTAssertCocoaError(try await archive.extract(fileEntry, to: archive.url!),
                            throwsErrorWithCode: .fileWriteFileExists)
        guard let linkEntry = try await archive.get("testZipItemLink") else {
            XCTFail("Failed to obtain test asset from archive.")
            return
        }

        let longFileName = String(repeating: ProcessInfo.processInfo.globallyUniqueString, count: 100)
        var overlongURL = URL(fileURLWithPath: NSTemporaryDirectory())
        overlongURL.appendPathComponent(longFileName)
        await XCTAssertPOSIXError(try await archive.extract(fileEntry, to: overlongURL),
                            throwsErrorWithCode: .ENAMETOOLONG)
        XCTAssertNotNil(linkEntry)
        await XCTAssertCocoaError(try await archive.extract(linkEntry, to: archive.url!),
                            throwsErrorWithCode: .fileWriteFileExists)
    }

    func testCorruptFileErrorConditions() async throws {
        let archiveURL = self.resourceURL(for: #function, pathExtension: "zip")
        let fileManager = FileManager()
        let destinationFileSystemRepresentation = fileManager.fileSystemRepresentation(withPath: archiveURL.path)
        let destinationFile: FILEPointer = fopen(destinationFileSystemRepresentation, "r+b")

        fseek(destinationFile, 64, SEEK_SET)
        // We have to inject a large enough zeroes block to guarantee that libcompression
        // detects the failure when reading the stream
        _ = try Data.write(chunk: Data(count: 512*1024), to: destinationFile)
        fclose(destinationFile)
        let archive = try await Archive(url: archiveURL, accessMode: .read)
        guard let entry = try await archive.get("data.random") else {
            XCTFail("Failed to read entry.")
            return
        }
        await XCTAssertSwiftError(try await archive.extract(entry, consumer: { _ in }),
                            throws: Data.CompressionError.corruptedData)
    }

    func testCorruptSymbolicLinkErrorConditions() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        for entry in try await archive.entries() {
            var tempFileURL = URL(fileURLWithPath: NSTemporaryDirectory())
            tempFileURL.appendPathComponent(ProcessInfo.processInfo.globallyUniqueString)
            await XCTAssertSwiftError(try await archive.extract(entry, to: tempFileURL),
                                throws: Archive.ArchiveError.invalidEntryPath)
        }
    }

    func testInvalidCompressionMethodErrorConditions() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        guard let entry = try await archive.get(".DS_Store") else {
            XCTFail("Missing entry in test archive")
            return
        }

        await XCTAssertSwiftError(try await archive.extract(entry, consumer: { (_) in }),
                            throws: Archive.ArchiveError.invalidCompressionMethod)
    }

    func testExtractEncryptedArchiveErrorConditions() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        var entriesRead = 0
        for entry in try await archive.entries() {
            entriesRead += 1
        }
        // We currently don't support encryption so we expect failed initialization for entry objects.
        XCTAssert(entriesRead == 0)
    }

    func testExtractInvalidBufferSizeErrorConditions() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        let entry = try await archive.get("text.txt")!
        await XCTAssertThrowsError(try await archive.extract(entry, to: URL(fileURLWithPath: ""), bufferSize: 0, skipCRC32: true))
        let archive2 = await self.archive(for: #function, mode: .read)
        let entry2 = try await archive2.get("text.txt")!
        await XCTAssertThrowsError(try await archive2.extract(entry2, bufferSize: 0, skipCRC32: true, consumer: { _ in }))
    }

    func testExtractUncompressedEmptyFile() async throws {
        // We had a logic error, where completion handlers for empty entries were not called
        // Ensure that this edge case works
        let didCallCompletion = SharedMutableValue(false)
        let archive = await self.archive(for: #function, mode: .read)
        guard let entry = try await archive.get("empty.txt") else { XCTFail("Failed to extract entry."); return }

        do {
            _ = try await archive.extract(entry) { (data) in
                XCTAssertEqual(data.count, 0)
                await didCallCompletion.set(true)
            }
        } catch {
            XCTFail("Unexpected error while trying to extract empty file of uncompressed archive.")
        }
        let didCallCompletionValue = await didCallCompletion.get()
        XCTAssert(didCallCompletionValue)
    }

    func testExtractUncompressedEntryCancelation() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        guard let entry = try await archive.get("original") else { XCTFail("Failed to extract entry."); return }
        let progress = archive.makeProgressForReading(entry)
        do {
            let readCount = SharedMutableValue(0)
            _ = try await archive.extract(entry, bufferSize: 1, progress: progress) { (data) in
                await readCount.increment(data.count)
                if await readCount.get() == 4 { progress.cancel() }
            }
        } catch let error as Archive.ArchiveError {
            XCTAssert(error == Archive.ArchiveError.cancelledOperation)
            XCTAssertEqual(progress.fractionCompleted, 0.5, accuracy: .ulpOfOne)
        } catch {
            XCTFail("Unexpected error while trying to cancel extraction.")
        }
    }

    func testExtractCompressedEntryCancelation() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        guard let entry = try await archive.get("random") else { XCTFail("Failed to extract entry."); return }
        let progress = archive.makeProgressForReading(entry)
        do {
            let readCount = SharedMutableValue(0)
            _ = try await archive.extract(entry, bufferSize: 256, progress: progress) { (data) in
                await readCount.increment(data.count)
                if await readCount.get() == 512 { progress.cancel() }
            }
        } catch let error as Archive.ArchiveError {
            XCTAssert(error == Archive.ArchiveError.cancelledOperation)
            XCTAssertEqual(progress.fractionCompleted, 0.5, accuracy: .ulpOfOne)
        } catch {
            XCTFail("Unexpected error while trying to cancel extraction.")
        }
    }

    func testProgressHelpers() async {
        let tempPath = NSTemporaryDirectory()
        var nonExistantURL = URL(fileURLWithPath: tempPath)
        nonExistantURL.appendPathComponent("invalid.path")
        let archive = await self.archive(for: #function, mode: .update)
        XCTAssert(archive.totalUnitCountForAddingItem(at: nonExistantURL) == -1)
    }

    func testDetectEntryType() async throws {
        let archive = await self.archive(for: #function, mode: .read)
        let expectedData: [String: Entry.EntryType] = [
            "META-INF/": .directory,
            "META-INF/container.xml": .file
        ]
        for entry in try await archive.entries() {
            XCTAssertEqual(entry.type, expectedData[entry.path])
        }
    }

    func testCRC32Check() async throws {
        let fileManager = FileManager()
        let archive = await self.archive(for: #function, mode: .read)
        let destinationURL = self.createDirectory(for: #function)
        await XCTAssertSwiftError(try await fileManager.unzipItem(at: archive.url!, to: destinationURL),
                            throws: Archive.ArchiveError.invalidCRC32)
    }

    func testSimpleTraversalAttack() async throws {
        let fileManager = FileManager()
        let archive = await self.archive(for: #function, mode: .read)
        let destinationURL = self.createDirectory(for: #function)
        await XCTAssertCocoaError(try await fileManager.unzipItem(at: archive.url!, to: destinationURL),
                            throwsErrorWithCode: .fileReadInvalidFileName)
    }

    func testPathDelimiterTraversalAttack() async throws {
        let fileManager = FileManager()
        let archive = await self.archive(for: #function, mode: .read)
        let destinationURL = self.createDirectory(for: #function)
        await XCTAssertCocoaError(try await fileManager.unzipItem(at: archive.url!, to: destinationURL),
                            throwsErrorWithCode: .fileReadInvalidFileName)
    }
}
