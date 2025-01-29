//
//  ZIPFoundationErrorConditionTests.swift
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

    func testArchiveInvalidEOCDRecordConditions() async {
        let emptyECDR = await Archive.EndOfCentralDirectoryRecord(data: Data(),
                                                            additionalDataProvider: {_ -> Data in
            return Data() })
        XCTAssertNil(emptyECDR)
        let invalidECDRData = Data(count: 22)
        let invalidECDR = await Archive.EndOfCentralDirectoryRecord(data: invalidECDRData,
                                                              additionalDataProvider: {_ -> Data in
            return Data() })
        XCTAssertNil(invalidECDR)
    }

    func testDirectoryCreationHelperMethods() {
        let processInfo = ProcessInfo.processInfo
        var nestedURL = ZIPFoundationTests.tempZipDirectoryURL
        nestedURL.appendPathComponent(processInfo.globallyUniqueString)
        nestedURL.appendPathComponent(processInfo.globallyUniqueString)
        do {
            try FileManager().createParentDirectoryStructure(for: nestedURL)
        } catch { XCTFail("Failed to create parent directory.") }
    }

    func testTemporaryReplacementDirectoryURL() async throws {
        let archive = await self.archive(for: #function, mode: .create)
        var tempURLs = Set<URL>()
        defer { for url in tempURLs { try? FileManager.default.removeItem(at: url) } }
        // We choose 2000 temp directories to test workaround for http://openradar.appspot.com/50553219
        for _ in 1...2000 {
            let tempDir = URL.temporaryReplacementDirectoryURL(for: archive)
            XCTAssertFalse(tempURLs.contains(tempDir), "Temp directory URL should be unique. \(tempDir)")
            tempURLs.insert(tempDir)
        }
    }
}

extension XCTestCase {

    func XCTAssertSwiftError<T, E: Error & Equatable>(_ expression: @autoclosure () async throws -> T,
                                                      throws error: E,
                                                      in file: StaticString = #file,
                                                      line: UInt = #line) async {
        var thrownError: Error?
        await XCTAssertThrowsError(try await expression(), file: file, line: line) { thrownError = $0}
        XCTAssertTrue(thrownError is E, "Unexpected error type: \(type(of: thrownError))", file: file, line: line)
        XCTAssertEqual(thrownError as? E, error, file: file, line: line)
    }

    func XCTAssertPOSIXError<T>(_ expression: @autoclosure () async throws -> T,
                                throwsErrorWithCode code: POSIXError.Code,
                                in file: StaticString = #file,
                                line: UInt = #line) async {
        var thrownError: POSIXError?
        await XCTAssertThrowsError(try await expression(), file: file, line: line) { thrownError = $0 as? POSIXError }
        XCTAssertNotNil(thrownError, file: file, line: line)
        XCTAssertTrue(thrownError?.code == code, file: file, line: line)
    }

    func XCTAssertCocoaError<T>(_ expression: @autoclosure () async throws -> T,
                                throwsErrorWithCode code: CocoaError.Code,
                                in file: StaticString = #file,
                                line: UInt = #line) async {
        var thrownError: CocoaError?
        #if os(macOS) || os(iOS) || os(tvOS) || os(visionOS) || os(watchOS)
        await XCTAssertThrowsError(try await expression(), file: file, line: line) { thrownError = $0 as? CocoaError}
        #else
        await XCTAssertThrowsError(try await expression(), file: file, line: line) {
            // For unknown reasons, some errors in the `NSCocoaErrorDomain` can't be cast to `CocoaError` on Linux.
            // We manually re-create them here as `CocoaError` to work around this.
            thrownError = CocoaError(.init(rawValue: ($0 as NSError).code))
        }
        #endif
        XCTAssertNotNil(thrownError, file: file, line: line)
        XCTAssertTrue(thrownError?.code == code, file: file, line: line)
    }
    
    func XCTAssertNoThrowAsync<T>(
        _ expression: @autoclosure () async throws -> T,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            _ = try await expression()
        } catch {
            XCTFail("Asynchronous call threw an error: \(error)", file: file, line: line)
        }
    }

    func XCTAssertThrowsError<T>(
        _ expression: @autoclosure () async throws -> T,
        file: StaticString = #filePath,
        line: UInt = #line,
        _ errorHandler: (_ error: Error) -> Void = { _ in }
    ) async {
        do {
            _ = try await expression()
            XCTFail("Asynchronous call did not throw an error.", file: file, line: line)
        } catch {
            errorHandler(error)
        }
    }
}
