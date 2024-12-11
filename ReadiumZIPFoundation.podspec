Pod::Spec.new do |s|
  s.name = 'ReadiumZIPFoundation'
  s.version = '1.0.0'
  s.license = 'MIT'
  s.summary = 'Effortless ZIP Handling in Swift'
  s.homepage = 'https://github.com/readium/ZIPFoundation'
  s.social_media_url = 'http://twitter.com/weichsel'
  s.authors = { 'Thomas Zoechling' => 'thomas@peakstep.com' }
  s.source = { :git => 'https://github.com/readium/ZIPFoundation.git', :tag => s.version }
  s.swift_versions = ['4.0', '4.2', '5.0']

  s.ios.deployment_target = '12.0'
  s.osx.deployment_target = '10.11'

  s.source_files = 'Sources/ZIPFoundation/*.swift'
  s.resource_bundles = {'ZIPFoundation_Privacy' => ['Sources/ZIPFoundation/Resources/PrivacyInfo.xcprivacy']}
end
