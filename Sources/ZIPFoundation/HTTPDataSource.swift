//
//  HTTPDataSource.swift
//  ZIPFoundation
//
//  Created by Mickaël on 12/18/24.
//

import Foundation

extension URLSession {
    func fetch(_ request: URLRequest) throws -> (URLResponse, Data?) {
        var result: Result<(URLResponse, Data?), Error>!

        let semaphore = DispatchSemaphore(value: 0)
        dataTask(with: request) { data, response, error in
            if let response = response {
                result = .success((response, data))
            } else {
                result = .failure(error!)
            }
            semaphore.signal()
        }.resume()
        
        _ = semaphore.wait(timeout: .distantFuture)
        
        return try result.get()
    }
}

extension URLRequest {
    static func head(_ url: URL) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = "HEAD"
        print("HEAD")
        return request
    }
    
    static func get(_ url: URL, range: Range<UInt64>) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        print("GET \(range) - \(range.count)")
        
        let start = max(0, range.lowerBound)
        var value = "\(start)-"
        if range.upperBound >= start {
            value += "\(range.upperBound)"
        }
        request.setValue("bytes=\(value)", forHTTPHeaderField: "Range")
        
        return request
    }
}

enum HTTPDataSourceError: Error {
    case noDataReceived
    case unexpectedDataLength
}

final class HTTPDataSource : DataSource {
    
    private let url: URL
    
    init(url: URL) throws {
        precondition(url.isHTTPURL)
        self.url = url
    }
    
    private var _length: UInt64?
    
    func length() throws -> UInt64 {
        if _length == nil {
            let (response, _) = try URLSession.shared.fetch(.head(url))
            _length = UInt64(response.expectedContentLength)
        }
        return _length!
    }
    
    private var _position: UInt64 = 0
    
    func position() throws -> UInt64 {
        _position
    }
    
    func seek(to position: UInt64) throws {
        _position = position
    }
    
    func read(length: Int) throws -> Data {
        guard length > 0 else {
            return Data()
        }
        
        let (_, data) = try URLSession.shared.fetch(.get(url, range: _position..<_position+UInt64(length - 1)))
        guard let data = data else {
            throw HTTPDataSourceError.noDataReceived
        }
        print("READ \(length) from \(_position) : \(data.base64EncodedString())")
        _position += UInt64(data.count)
        return data
    }
    
    func close() throws {
    }
}

extension URL {
    var isHTTPURL: Bool {
        let scheme = scheme?.lowercased()
        return scheme == "http" || scheme == "https"
    }
}
