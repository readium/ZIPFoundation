// swift-tools-version:5.9
import PackageDescription

#if canImport(Compression)
let targets: [Target] = [
    .target(name: "ReadiumZIPFoundation",
            resources: [
                .copy("Resources/PrivacyInfo.xcprivacy")
            ]),
    .testTarget(name: "ReadiumZIPFoundationTests", dependencies: ["ReadiumZIPFoundation"])
]
#else
let targets: [Target] = [
    .systemLibrary(name: "CZLib", pkgConfig: "zlib", providers: [.brew(["zlib"]), .apt(["zlib"])]),
    .target(name: "ReadiumZIPFoundation", dependencies: ["CZLib"], cSettings: [.define("_GNU_SOURCE", to: "1")]),
    .testTarget(name: "ReadiumZIPFoundationTests", dependencies: ["ReadiumZIPFoundation"])
]
#endif

let package = Package(
    name: "ReadiumZIPFoundation",
    platforms: [
        .macOS(.v10_13), .iOS(.v12), .tvOS(.v12), .watchOS(.v4), .visionOS(.v1)
    ],
    products: [
        .library(name: "ReadiumZIPFoundation", targets: ["ReadiumZIPFoundation"])
    ],
    targets: targets,
    swiftLanguageVersions: [.v4, .v4_2, .v5]
)
