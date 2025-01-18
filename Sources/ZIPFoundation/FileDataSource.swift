//
//  FileDataSource.swift
//  ZIPFoundation
//
//  Created by Mickaël on 12/17/24.
//

import Foundation

/// A `DataSource` working with a ZIP file on the file system.
actor FileDataSource : WritableDataSource {
    
    enum AccessMode: String {
        case read = "rb"
        case write = "rb+"
    }
    
    let file: FILEPointer
    var isClosed: Bool = false
    
    init(url: URL, mode: AccessMode) async throws {
        precondition(url.isFileURL)
        
        let fsRepr = FileManager.default.fileSystemRepresentation(withPath: url.path)
        guard let file = fopen(fsRepr, mode.rawValue) else {
            throw POSIXError(errno, path: url.path)
        }
        
        self.init(file: file)
        
        setvbuf(file, nil, _IOFBF, Int(defaultPOSIXBufferSize))
        try checkNoError()
        
        try await seek(to: 0)
    }
    
    init(file: FILEPointer) {
        self.file = file
    }
    
    func length() async throws -> UInt64 {
        let currentPos = try await position()
        fseeko(file, 0, SEEK_END)
        try checkNoError()
        let length = try await position()
        try await seek(to: currentPos)
        return length
    }
    
    func position() async throws -> UInt64 {
        let position = ftello(file)
        try checkNoError()
        return UInt64(position)
    }
    
    func seek(to position: UInt64) async throws {
        fseeko(file, off_t(position), SEEK_SET)
        try checkNoError()
    }
    
    func read(length: Int) async throws -> Data {
        let alignment = MemoryLayout<UInt>.alignment
        let bytes = UnsafeMutableRawPointer.allocate(byteCount: length, alignment: alignment)
        let bytesRead = fread(bytes, 1, length, file)
        try checkNoError()
        return Data(
            bytesNoCopy: bytes,
            count: bytesRead,
            deallocator: .custom({ buf, _ in buf.deallocate() })
        )
    }
    
    func write(_ data: Data) async throws {
        try data.withUnsafeBytes { rawBufferPointer in
            if let baseAddress = rawBufferPointer.baseAddress, rawBufferPointer.count > 0 {
                let pointer = baseAddress.assumingMemoryBound(to: UInt8.self)
                fwrite(pointer, 1, data.count, file)
                try checkNoError()
            }
        }
    }

    func writeLargeChunk(_ data: Data, size: UInt64, bufferSize: Int) async throws {
        var sizeWritten: UInt64 = 0
        try data.withUnsafeBytes { rawBufferPointer in
            if let baseAddress = rawBufferPointer.baseAddress, rawBufferPointer.count > 0 {
                let pointer = baseAddress.assumingMemoryBound(to: UInt8.self)
                
                while sizeWritten < size {
                    let remainingSize = size - sizeWritten
                    let chunkSize = Swift.min(Int(remainingSize), bufferSize)
                    let curPointer = pointer.advanced(by: Int(sizeWritten))
                    fwrite(curPointer, 1, chunkSize, file)
                    try checkNoError()
                    sizeWritten += UInt64(chunkSize)
                }
            }
        }
    }
    
    func truncate(to length: UInt64) async throws {
        ftruncate(fileno(file), off_t(length))
        try checkNoError()
    }
    
    func flush() async throws {
        fflush(file)
        try checkNoError()
    }
    
    nonisolated func close() {
        Task {
            do {
                try await close()
            } catch {
                print(error)
            }
        }
    }
    
    private func close() async throws {
        guard !isClosed else {
            return
        }
        fclose(file)
        try checkNoError()
        isClosed = true
    }
    
    private func checkNoError() throws {
        let code = ferror(file)
        guard code > 0 else {
            return
        }
        clearerr(file)
        
        throw POSIXError(POSIXError.Code(rawValue: code) ?? .EPERM)
    }
}
