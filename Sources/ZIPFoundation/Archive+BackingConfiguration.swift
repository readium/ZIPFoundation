//
//  Archive+BackingConfiguration.swift
//  ZIPFoundation
//
//  Copyright © 2017-2024 Thomas Zoechling, https://www.peakstep.com and the ZIP Foundation project authors.
//  Released under the MIT License.
//
//  See https://github.com/weichsel/ZIPFoundation/blob/master/LICENSE for license information.
//

import Foundation

extension Archive {

    struct BackingConfiguration {
        let dataSource: DataSource
        let endOfCentralDirectoryRecord: EndOfCentralDirectoryRecord
        let zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory?
        #if swift(>=5.0)
        let memoryFile: MemoryFile?

        init(dataSource: DataSource,
             endOfCentralDirectoryRecord: EndOfCentralDirectoryRecord,
             zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory? = nil,
             memoryFile: MemoryFile? = nil) {
            self.dataSource = dataSource
            self.endOfCentralDirectoryRecord = endOfCentralDirectoryRecord
            self.zip64EndOfCentralDirectory = zip64EndOfCentralDirectory
            self.memoryFile = memoryFile
        }
        #else

        init(dataSource: DataSource,
             endOfCentralDirectoryRecord: EndOfCentralDirectoryRecord,
             zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory?) {
            self.dataSource = dataSource
            self.endOfCentralDirectoryRecord = endOfCentralDirectoryRecord
            self.zip64EndOfCentralDirectory = zip64EndOfCentralDirectory
        }
        #endif
    }

    static func makeBackingConfiguration(for url: URL, mode: AccessMode) throws
    -> BackingConfiguration {
        let dataSource: DataSource
        switch mode {
        case .read:
            dataSource = try FileDataSource(url: url, mode: .read)
        case .create:
            let endOfCentralDirectoryRecord = EndOfCentralDirectoryRecord(numberOfDisk: 0, numberOfDiskStart: 0,
                                                                          totalNumberOfEntriesOnDisk: 0,
                                                                          totalNumberOfEntriesInCentralDirectory: 0,
                                                                          sizeOfCentralDirectory: 0,
                                                                          offsetToStartOfCentralDirectory: 0,
                                                                          zipFileCommentLength: 0,
                                                                          zipFileCommentData: Data())
            try endOfCentralDirectoryRecord.data.write(to: url, options: .withoutOverwriting)
            fallthrough
        case .update:
            dataSource = try FileDataSource(url: url, mode: .write)
        }
        
        guard let (eocdRecord, zip64EOCD) = try Archive.scanForEndOfCentralDirectoryRecord(in: dataSource) else {
            throw ArchiveError.missingEndOfCentralDirectoryRecord
        }
        try dataSource.seek(to: 0)
        
        return BackingConfiguration(
            dataSource: dataSource,
            endOfCentralDirectoryRecord: eocdRecord,
            zip64EndOfCentralDirectory: zip64EOCD
        )
    }

    #if swift(>=5.0)
    static func makeBackingConfiguration(for data: Data, mode: AccessMode) throws
    -> BackingConfiguration {
        let posixMode: String
        switch mode {
        case .read: posixMode = "rb"
        case .create: posixMode = "wb+"
        case .update: posixMode = "rb+"
        }
        let memoryFile = MemoryFile(data: data)
        guard let archiveFile = memoryFile.open(mode: posixMode) else {
            throw ArchiveError.unreadableArchive
        }
        
        let dataSource = FileDataSource(file: archiveFile)

        if mode == .create {
            let endOfCentralDirectoryRecord = EndOfCentralDirectoryRecord(numberOfDisk: 0, numberOfDiskStart: 0,
                                                                          totalNumberOfEntriesOnDisk: 0,
                                                                          totalNumberOfEntriesInCentralDirectory: 0,
                                                                          sizeOfCentralDirectory: 0,
                                                                          offsetToStartOfCentralDirectory: 0,
                                                                          zipFileCommentLength: 0,
                                                                          zipFileCommentData: Data())
            try dataSource.write(endOfCentralDirectoryRecord.data)
        }
        
        guard let (eocdRecord, zip64EOCD) = try Archive.scanForEndOfCentralDirectoryRecord(in: dataSource) else {
            throw ArchiveError.missingEndOfCentralDirectoryRecord
        }

        try dataSource.seek(to: 0)
        return BackingConfiguration(dataSource: dataSource,
                                    endOfCentralDirectoryRecord: eocdRecord,
                                    zip64EndOfCentralDirectory: zip64EOCD,
                                    memoryFile: memoryFile)
    }
    #endif
}
