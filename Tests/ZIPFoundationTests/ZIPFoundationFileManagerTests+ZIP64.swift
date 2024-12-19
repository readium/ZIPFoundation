//
//  ZIPFoundationFileManagerTests+ZIP64.swift
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

    private enum ZIP64FileManagerTestsError: Error, CustomStringConvertible {
        case failedToZipItem(url: URL)
        case failedToReadArchive(url: URL)
        case failedToUnzipItem

        var description: String {
            switch self {
            case .failedToZipItem(let assetURL):
                return "Failed to zip item at URL: \(assetURL)."
            case .failedToReadArchive(let fileArchiveURL):
                return "Failed to read archive at URL: \(fileArchiveURL)."
            case .failedToUnzipItem:
                return "Failed to unzip item."
            }
        }
    }

    func testZipCompressedZIP64Item() async {
        do {
            try await archiveZIP64Item(for: #function, compressionMethod: .deflate)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testZipUncompressedZIP64Item() async {
        do {
            try await archiveZIP64Item(for: #function, compressionMethod: .none)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testUnzipCompressedZIP64Item() async {
        // stored by zip 3.0 via command line: zip -0 -fz
        //
        // testUnzipCompressedZIP64Item.zip/
        //   ├─ directory
        //   ├─ testLink
        //   ├─ nested
        //     ├─ nestedLink
        //     ├─ faust copy.txt
        //     ├─ deep
        //       ├─ another.random
        //   ├─ faust.txt
        //   ├─ empty
        //   ├─ data.random
        //   ├─ random.data
        do {
            try await unarchiveZIP64Item(for: #function)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testUnzipUncompressedZIP64Item() async {
        // stored by zip 3.0 via command line: zip -0 -fz
        //
        // testUnzipCompressedZIP64Item.zip/
        //   ├─ directory
        //   ├─ testLink
        //   ├─ nested
        //     ├─ nestedLink
        //     ├─ faust copy.txt
        //     ├─ deep
        //       ├─ another.random
        //   ├─ faust.txt
        //   ├─ empty
        //   ├─ data.random
        //   ├─ random.data
        do {
            try await unarchiveZIP64Item(for: #function)
        } catch {
            XCTFail("\(error)")
        }
    }

    func testUnzipItemWithZIP64DataDescriptor() async {
        // testUnzipCompressedZIP64Item.zip
        //   ├─ simple.data
        do {
            try await unarchiveZIP64Item(for: #function)
        } catch {
            XCTFail("\(error)")
        }
    }

    // MARK: - Helpers

    private func archiveZIP64Item(for testFunction: String, compressionMethod: CompressionMethod) async throws {
        self.mockIntMaxValues(int32Factor: 16, int16Factor: 16)
        defer { self.resetIntMaxValues() }
        let assetURL = self.resourceURL(for: testFunction, pathExtension: "png")
        var fileArchiveURL = ZIPFoundationTests.tempZipDirectoryURL
        fileArchiveURL.appendPathComponent(self.archiveName(for: testFunction))
        do {
            try await FileManager().zipItem(at: assetURL, to: fileArchiveURL, compressionMethod: compressionMethod)
        } catch {
            throw ZIP64FileManagerTestsError.failedToZipItem(url: assetURL)
        }
        let archive = try await Archive(url: fileArchiveURL, accessMode: .read)
        let entry = try await archive.get(assetURL.lastPathComponent)
        XCTAssertNotNil(entry)
        await archive.checkIntegrity()
    }

    private func unarchiveZIP64Item(for testFunction: String) async throws {
        let fileManager = FileManager()
        let archive = await self.archive(for: testFunction, mode: .read)
        let destinationURL = self.createDirectory(for: testFunction)
        do {
            try await fileManager.unzipItem(at: archive.url!, to: destinationURL)
        } catch {
            throw ZIP64FileManagerTestsError.failedToUnzipItem
        }
        var itemsExist = false
        for try await entry in archive {
            let directoryURL = destinationURL.appendingPathComponent(entry.path)
            itemsExist = fileManager.itemExists(at: directoryURL)
            if !itemsExist { break }
        }
        XCTAssert(itemsExist)
    }
}
