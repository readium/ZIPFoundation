//
//  FileDataSource.swift
//  ZIPFoundation
//
//  Created by Mickaël on 12/17/24.
//

import Foundation

/// A `DataSource` working with a ZIP file on the file system.
final class FileDataSource: DataSource {
    
    let isWritable: Bool
    private let url: URL
    private let length: UInt64
    
    init(url: URL, isWritable: Bool) async throws {
        precondition(url.isFileURL)
        
        let file = try url.open(mode: .read)
        fseeko(file, 0, SEEK_END)
        try file.checkNoError()
        let length = UInt64(ftello(file))
        try file.checkNoError()
        fclose(file)

        self.url = url
        self.isWritable = isWritable
        self.length = UInt64(length)
    }
    
    func length() async throws -> UInt64 { length }
    
    func openRead() async throws -> any DataSourceTransaction {
        try await FileDataSourceTransaction(url: url, mode: .read)
    }
    
    func openWrite() async throws -> any WritableDataSourceTransaction {
        guard isWritable else {
            throw DataSourceError.notWritable
        }
        return try await FileDataSourceTransaction(url: url, mode: .write)
    }
}

private enum FileAccessMode: String {
    case read = "rb"
    case write = "rb+"
}

private actor FileDataSourceTransaction: WritableDataSourceTransaction {

    private let file: FILEPointer
    
    init(url: URL, mode: FileAccessMode) async throws {
        self.file = try url.open(mode: mode)

        setvbuf(file, nil, _IOFBF, Int(defaultPOSIXBufferSize))
        try checkNoError()
        
        try await seek(to: 0)
    }
    
    deinit {
        fclose(file)
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
    
    private func checkNoError() throws {
        let code = ferror(file)
        guard code > 0 else {
            return
        }
        clearerr(file)
        
        throw POSIXError(POSIXError.Code(rawValue: code) ?? .EPERM)
    }
}

private extension URL {
    func open(mode: FileAccessMode) throws -> FILEPointer {
        let fsRepr = FileManager.default.fileSystemRepresentation(withPath: path)
        guard let file = fopen(fsRepr, mode.rawValue) else {
            throw POSIXError(errno, path: path)
        }
        return file
    }
}

private extension FILEPointer {
    func checkNoError() throws {
        let code = ferror(self)
        guard code > 0 else {
            return
        }
        clearerr(self)
        
        throw POSIXError(POSIXError.Code(rawValue: code) ?? .EPERM)
    }
}
