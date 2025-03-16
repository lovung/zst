const std = @import("std");

/// A Swiss Table (flat hash map) implementation with open addressing and group probing.
/// Generic over key and value types, with configurable parameters.
pub fn SwissTable(
    comptime K: type,
    comptime V: type,
) type {
    return struct {
        /// Default hash function that wraps Wyhash
        fn defaultHash() *const fn (key: K) u64 {
            const S = struct {
                pub fn hash(key: K) u64 {
                    const seed = 0;
                    return std.hash.Wyhash.hash(seed, std.mem.asBytes(&key));
                }
            };
            return &S.hash;
        }

        /// Configuration struct for SwissTable
        pub const Config = struct {
            /// Size of the probing group
            groupSize: usize,
            /// Maximum load factor before resizing (between 0 and 1)
            maxLoadFactor: f32,
            /// Allocator for memory management
            allocator: std.mem.Allocator,
            /// Custom hash function, defaults to defaultHash
            hashFn: *const fn (key: K) u64,

            /// Creates a Config instance with default values
            pub fn default(allocator: std.mem.Allocator) Config {
                return .{
                    .groupSize = 16, // Default group size
                    .maxLoadFactor = 0.875, // Default max load factor (87.5%)
                    .allocator = allocator, // User-provided allocator
                    .hashFn = defaultHash(), // Default hash function
                };
            }
        };

        const Self = @This();

        const Slot = struct {
            key: K,
            value: V,
        };

        config: Config,
        ctrl: []u8,
        slots: []Slot,
        cap: usize,
        size: usize,

        /// Error set for SwissTable operations
        pub const Error = error{
            OutOfMemory,
            CapacityOverflow,
            InvalidConfig,
        };

        /// Iterator over key-value pairs
        pub const Iterator = struct {
            table: *const Self,
            index: usize,

            pub fn next(self: *Iterator) ?struct { K, V } {
                while (self.index < self.table.cap) : (self.index += 1) {
                    if (self.table.ctrl[self.index] != 0 and
                        self.table.ctrl[self.index] != 0x80)
                    {
                        defer self.index += 1;
                        return .{
                            self.table.slots[self.index].key,
                            self.table.slots[self.index].value,
                        };
                    }
                }
                return null;
            }
        };

        /// Initialize a new Swiss Table with given allocator and initial capacity
        pub fn init(config: Config, initCap: usize) Error!Self {
            // Validate config
            if (config.groupSize == 0) return error.InvalidConfig;
            if (config.maxLoadFactor <= 0.0 or config.maxLoadFactor >= 1.0) return error.InvalidConfig;

            const cap = std.math.ceilPowerOfTwo(usize, initCap) catch |err| switch (err) {
                error.Overflow => return error.CapacityOverflow,
            };
            if (cap == 0) return error.CapacityOverflow;

            const ctrl = try config.allocator.alloc(u8, cap + config.groupSize);
            errdefer config.allocator.free(ctrl);

            const slots = try config.allocator.alloc(Slot, cap);
            errdefer config.allocator.free(slots);

            @memset(ctrl[0..cap], 0);
            @memset(ctrl[cap .. cap + config.groupSize], 0xFF);

            return Self{
                .config = config,
                .ctrl = ctrl,
                .slots = slots,
                .cap = cap,
                .size = 0,
            };
        }

        /// Free all allocated memory
        pub fn deinit(self: *Self) void {
            self.config.allocator.free(self.ctrl);
            self.config.allocator.free(self.slots);
            self.* = undefined;
        }

        /// Get current number of elements
        pub fn count(self: *const Self) usize {
            return self.size;
        }

        /// Get current capacity
        pub fn capacity(self: *const Self) usize {
            return self.cap;
        }

        /// Clear all entries without freeing memory
        pub fn clear(self: *Self) void {
            @memset(self.ctrl[0..self.cap], 0);
            self.size = 0;
        }

        /// Get an iterator over all key-value pairs
        pub fn iterator(self: *const Self) Iterator {
            return .{ .table = self, .index = 0 };
        }

        fn hashToCtrl(h: u64) u8 {
            const r: u8 = @truncate((h >> 57) & 0x7F);
            return r | 0x01;
        }

        fn probeStart(self: *const Self, hash: u64) usize {
            return @truncate(hash & (self.cap - 1));
        }

        fn groupProbe(self: *const Self, pos: usize) usize {
            return (pos + self.config.groupSize - 1) & (self.cap - 1);
        }

        /// Insert or update a key-value pair
        pub fn put(self: *Self, key: K, value: V) Error!void {
            if (@as(f32, @floatFromInt(self.size)) >=
                @as(f32, @floatFromInt(self.cap)) * self.config.maxLoadFactor)
            {
                try self.grow();
            }

            const hash = self.config.hashFn(key);
            const ctrlByte = hashToCtrl(hash);
            var pos = self.probeStart(hash);

            while (true) {
                for (0..self.config.groupSize) |i| {
                    const idx = (pos + i) & (self.cap - 1);
                    const ctrl = self.ctrl[idx];

                    if (ctrl == 0) {
                        self.ctrl[idx] = ctrlByte;
                        self.slots[idx] = .{ .key = key, .value = value };
                        self.size += 1;
                        return;
                    }

                    if (ctrl == ctrlByte and std.meta.eql(self.slots[idx].key, key)) {
                        self.slots[idx].value = value;
                        return;
                    }
                }
                pos = self.groupProbe(pos);
            }
        }

        /// Get value associated with key, if it exists
        pub fn get(self: *const Self, key: K) ?V {
            const hash = self.config.hashFn(key);
            const ctrlByte = hashToCtrl(hash);
            var pos = self.probeStart(hash);

            while (true) {
                for (0..self.config.groupSize) |i| {
                    const idx = (pos + i) & (self.cap - 1);
                    const ctrl = self.ctrl[idx];

                    if (ctrl == 0) return null;
                    if (ctrl == ctrlByte and std.meta.eql(self.slots[idx].key, key)) {
                        return self.slots[idx].value;
                    }
                }
                pos = self.groupProbe(pos);
            }
        }

        /// Remove a key-value pair, returns true if removed
        pub fn remove(self: *Self, key: K) bool {
            const hash = self.config.hashFn(key);
            const ctrlByte = hashToCtrl(hash);
            var pos = self.probeStart(hash);

            while (true) {
                for (0..self.config.groupSize) |i| {
                    const idx = (pos + i) & (self.cap - 1);
                    const ctrl = self.ctrl[idx];

                    if (ctrl == 0) return false;
                    if (ctrl == ctrlByte and std.meta.eql(self.slots[idx].key, key)) {
                        self.ctrl[idx] = 0x80;
                        self.size -= 1;
                        return true;
                    }
                }
                pos = self.groupProbe(pos);
            }
        }

        fn grow(self: *Self) Error!void {
            const old = Self{
                .config = self.config,
                .ctrl = self.ctrl,
                .slots = self.slots,
                .cap = self.cap,
                .size = self.size,
            };

            self.cap *= 2;
            if (self.cap == 0) return error.CapacityOverflow;

            self.size = 0;
            self.ctrl = try self.config.allocator.alloc(u8, self.cap + self.config.groupSize);
            errdefer self.config.allocator.free(self.ctrl);

            self.slots = try self.config.allocator.alloc(Slot, self.cap);
            errdefer self.config.allocator.free(self.slots);

            @memset(self.ctrl[0..self.cap], 0);
            @memset(self.ctrl[self.cap .. self.cap + self.config.groupSize], 0xFF);

            var it = old.iterator();
            while (it.next()) |entry| {
                try self.put(entry[0], entry[1]);
            }

            @constCast(&old).deinit();
        }
    };
}

// Unit tests
test "SwissTable functionality" {
    const allocator = std.testing.allocator;
    const Map = SwissTable(u32, []const u8);
    const config = Map.Config.default(allocator);

    // Test initialization
    var map = try Map.init(config, 16);
    defer map.deinit();
    try std.testing.expectEqual(@as(usize, 16), map.capacity());
    try std.testing.expectEqual(@as(usize, 0), map.count());

    // Test put and get
    try map.put(1, "one");
    try map.put(2, "two");
    try std.testing.expectEqual(@as(usize, 2), map.count());
    try std.testing.expectEqualStrings("one", map.get(1).?);
    try std.testing.expectEqualStrings("two", map.get(2).?);
    try std.testing.expect(map.get(3) == null);

    // Test update existing key
    try map.put(1, "updated");
    try std.testing.expectEqual(@as(usize, 2), map.count()); // Count shouldn't increase
    try std.testing.expectEqualStrings("updated", map.get(1).?);

    // Test remove
    try std.testing.expect(map.remove(1));
    try std.testing.expectEqual(@as(usize, 1), map.count());
    try std.testing.expect(map.get(1) == null);
    try std.testing.expect(!map.remove(1)); // Remove non-existent key

    // Test iterator
    try map.put(3, "three");
    var it = map.iterator();
    var found_two = false;
    var found_three = false;
    var iter_count: usize = 0;
    while (it.next()) |entry| {
        iter_count += 1;
        if (entry[0] == 2) {
            found_two = true;
            try std.testing.expectEqualStrings("two", entry[1]);
        } else if (entry[0] == 3) {
            found_three = true;
            try std.testing.expectEqualStrings("three", entry[1]);
        }
    }
    try std.testing.expectEqual(@as(usize, 2), iter_count);
    try std.testing.expect(found_two);
    try std.testing.expect(found_three);

    // Test clear
    map.clear();
    try std.testing.expectEqual(@as(usize, 0), map.count());
    try std.testing.expect(map.get(2) == null);
    try std.testing.expect(map.get(3) == null);
}

test "SwissTable with custom hash function" {
    const allocator = std.testing.allocator;
    const Map = SwissTable(u32, []const u8);

    // Simple custom hash function that uses XOR and shifts
    const CustomHasher = struct {
        pub fn hash(key: u32) u64 {
            var x = @as(u64, key);
            x ^= x >> 33;
            x *%= 0xff51afd7ed558ccd;
            x ^= x >> 33;
            x *%= 0xc4ceb9fe1a85ec53;
            x ^= x >> 33;
            return x;
        }
    };

    var config = Map.Config.default(allocator);
    config.hashFn = &CustomHasher.hash;

    // Test initialization
    var map = try Map.init(config, 16);
    defer map.deinit();

    // Basic functionality test with custom hash
    try map.put(1, "one");
    try map.put(2, "two");
    try map.put(3, "three");

    try std.testing.expectEqualStrings("one", map.get(1).?);
    try std.testing.expectEqualStrings("two", map.get(2).?);
    try std.testing.expectEqualStrings("three", map.get(3).?);
    try std.testing.expect(map.get(4) == null);

    // Test collisions still work
    try map.put(1, "updated");
    try std.testing.expectEqualStrings("updated", map.get(1).?);

    // Test remove with custom hash
    try std.testing.expect(map.remove(2));
    try std.testing.expect(!map.remove(2)); // Already removed
    try std.testing.expect(map.get(2) == null);
}
