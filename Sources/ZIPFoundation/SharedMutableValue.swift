//
//  Copyright 2025 Readium Foundation. All rights reserved.
//  Use of this source code is governed by the BSD-style license
//  available in the top-level LICENSE file of the project.
//

import Foundation

actor SharedMutableValue<Value> {
    private var value: Value
    
    init(_ value: Value) {
        self.value = value
    }
    
    func get() -> Value {
        value
    }
    
    func set(_ newValue: Value) {
        value = newValue
    }
}

extension SharedMutableValue where Value: Numeric {
    
    init() {
        self.init(0)
    }
    
    func increment(_ i: Value) {
        value += i
    }
}
