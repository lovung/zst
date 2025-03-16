const std = @import("std");

/// Configuration struct for SwissTable
pub const Config = struct {
    /// Size of the probing group
    group_size: usize,
    /// Maximum load factor before resizing (between 0 and 1)
    max_load_factor: f32,
    /// Allocator for memory management
    allocator: std.mem.Allocator,

    /// Creates a Config instance with default values
    pub fn default(allocator: std.mem.Allocator) Config {
        return .{
            .group_size = 16, // Default group size
            .max_load_factor = 0.875, // Default max load factor (87.5%)
            .allocator = allocator, // User-provided allocator
        };
    }
};

/// A Swiss Table (flat hash map) implementation with open addressing and group probing.
/// Generic over key and value types, with configurable parameters.
pub fn SwissTable(
    comptime K: type,
    comptime V: type,
    comptime config: Config,
) type {
    return struct {
        const Self = @This();

        // Validate config
        comptime {
            std.debug.assert(config.group_size > 0);
            std.debug.assert(config.max_load_factor > 0.0 and config.max_load_factor < 1.0);
        }

        const Slot = struct {
            key: K,
            value: V,
        };

        ctrl: []u8,
        slots: []Slot,
        cap: usize,
        size: usize,

        /// Error set for SwissTable operations
        pub const Error = error{
            OutOfMemory,
            CapacityOverflow,
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
        pub fn init(initial_capacity: usize) Error!Self {
            const cap = std.math.ceilPowerOfTwo(usize, initial_capacity) catch |err| switch (err) {
                error.Overflow => return error.CapacityOverflow,
            };
            if (cap == 0) return error.CapacityOverflow;

            const ctrl = try config.allocator.alloc(u8, cap + config.group_size);
            errdefer config.allocator.free(ctrl);

            const slots = try config.allocator.alloc(Slot, cap);
            errdefer config.allocator.free(slots);

            @memset(ctrl[0..cap], 0);
            @memset(ctrl[cap .. cap + config.group_size], 0xFF);

            return Self{
                .ctrl = ctrl,
                .slots = slots,
                .cap = cap,
                .size = 0,
            };
        }

        /// Free all allocated memory
        pub fn deinit(self: *Self) void {
            config.allocator.free(self.ctrl);
            config.allocator.free(self.slots);
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
            return (pos + config.group_size - 1) & (self.cap - 1);
        }

        /// Insert or update a key-value pair
        pub fn put(self: *Self, key: K, value: V) Error!void {
            if (@as(f32, @floatFromInt(self.size)) >=
                @as(f32, @floatFromInt(self.cap)) * config.max_load_factor)
            {
                try self.grow();
            }

            const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
            const ctrl_byte = hashToCtrl(hash);
            var pos = self.probeStart(hash);

            while (true) {
                for (0..config.group_size) |i| {
                    const idx = (pos + i) & (self.cap - 1);
                    const ctrl = self.ctrl[idx];

                    if (ctrl == 0) {
                        self.ctrl[idx] = ctrl_byte;
                        self.slots[idx] = .{ .key = key, .value = value };
                        self.size += 1;
                        return;
                    }

                    if (ctrl == ctrl_byte and std.meta.eql(self.slots[idx].key, key)) {
                        self.slots[idx].value = value;
                        return;
                    }
                }
                pos = self.groupProbe(pos);
            }
        }

        /// Get value associated with key, if it exists
        pub fn get(self: *const Self, key: K) ?V {
            const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
            const ctrl_byte = hashToCtrl(hash);
            var pos = self.probeStart(hash);

            while (true) {
                for (0..config.group_size) |i| {
                    const idx = (pos + i) & (self.cap - 1);
                    const ctrl = self.ctrl[idx];

                    if (ctrl == 0) return null;
                    if (ctrl == ctrl_byte and std.meta.eql(self.slots[idx].key, key)) {
                        return self.slots[idx].value;
                    }
                }
                pos = self.groupProbe(pos);
            }
        }

        /// Remove a key-value pair, returns true if removed
        pub fn remove(self: *Self, key: K) bool {
            const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
            const ctrl_byte = hashToCtrl(hash);
            var pos = self.probeStart(hash);

            while (true) {
                for (0..config.group_size) |i| {
                    const idx = (pos + i) & (self.cap - 1);
                    const ctrl = self.ctrl[idx];

                    if (ctrl == 0) return false;
                    if (ctrl == ctrl_byte and std.meta.eql(self.slots[idx].key, key)) {
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
                .ctrl = self.ctrl,
                .slots = self.slots,
                .cap = self.cap,
                .size = self.size,
            };

            self.cap *= 2;
            if (self.cap == 0) return error.CapacityOverflow;

            self.size = 0;
            self.ctrl = try config.allocator.alloc(u8, self.cap + config.group_size);
            errdefer config.allocator.free(self.ctrl);

            self.slots = try config.allocator.alloc(Slot, self.cap);
            errdefer config.allocator.free(self.slots);

            @memset(self.ctrl[0..self.cap], 0);
            @memset(self.ctrl[self.cap .. self.cap + config.group_size], 0xFF);

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
    const Map = SwissTable(u32, []const u8, Config.default(allocator));

    // Test initialization
    var map = try Map.init(16);
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
