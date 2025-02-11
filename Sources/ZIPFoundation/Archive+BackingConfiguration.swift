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

        init(
            dataSource: DataSource,
            endOfCentralDirectoryRecord: EndOfCentralDirectoryRecord,
            zip64EndOfCentralDirectory: ZIP64EndOfCentralDirectory?
        ) {
            self.dataSource = dataSource
            self.endOfCentralDirectoryRecord = endOfCentralDirectoryRecord
            self.zip64EndOfCentralDirectory = zip64EndOfCentralDirectory
        }
    }
    
    static func makeBackingConfiguration(for url: URL, mode: AccessMode) async throws -> BackingConfiguration {
        let dataSource: DataSource
        switch mode {
        case .read:
            dataSource = try await FileDataSource(url: url, isWritable: false)
        case .create:
            let endOfCentralDirectoryRecord = EndOfCentralDirectoryRecord(
                numberOfDisk: 0, numberOfDiskStart: 0,
                totalNumberOfEntriesOnDisk: 0,
                totalNumberOfEntriesInCentralDirectory: 0,
                sizeOfCentralDirectory: 0,
                offsetToStartOfCentralDirectory: 0,
                zipFileCommentLength: 0,
                zipFileCommentData: Data()
            )
            try endOfCentralDirectoryRecord.data.write(to: url, options: .withoutOverwriting)
            fallthrough
        case .update:
            dataSource = try await FileDataSource(url: url, isWritable: true)
        }
        
        return try await makeBackingConfiguration(for: dataSource)
    }
    
    static func makeBackingConfiguration(for dataSource: DataSource) async throws -> BackingConfiguration {
        guard let (eocdRecord, zip64EOCD) = try await Archive.scanForEndOfCentralDirectoryRecord(in: dataSource) else {
            throw ArchiveError.missingEndOfCentralDirectoryRecord
        }
        
        return BackingConfiguration(
            dataSource: dataSource,
            endOfCentralDirectoryRecord: eocdRecord,
            zip64EndOfCentralDirectory: zip64EOCD
        )
    }
}
