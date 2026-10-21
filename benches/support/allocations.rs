//! Measurement-only allocator shim, compiled only with `allocation-profile`.
//! Production library source forbids unsafe code. Profile timings are not speed evidence.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
struct Meter;
#[global_allocator]
static ALLOCATOR: Meter = Meter;

fn allocated(size: usize) {
    CALLS.fetch_add(1, Ordering::Relaxed);
    REQUESTED.fetch_add(size, Ordering::Relaxed);
    let live = LIVE.fetch_add(size, Ordering::Relaxed).saturating_add(size);
    PEAK.fetch_max(live, Ordering::Relaxed);
}
// SAFETY: Every allocation/deallocation is forwarded unchanged to the system
// allocator. Atomic counters neither inspect nor alter allocated memory.
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc supplies a valid layout; System owns the allocation.
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() && ENABLED.load(Ordering::Relaxed) {
            allocated(layout.size());
        }
        result
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The layout and returned memory have System's original contract.
        let result = unsafe { System.alloc_zeroed(layout) };
        if !result.is_null() && ENABLED.load(Ordering::Relaxed) {
            allocated(layout.size());
        }
        result
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        // SAFETY: GlobalAlloc supplies the original System pointer and layout.
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Forward the original pointer/layout and requested size unchanged.
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        if !result.is_null() && ENABLED.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            allocated(new_size);
        }
        result
    }
}
pub fn start() {
    assert!(!ENABLED.load(Ordering::Relaxed));
    for count in [&CALLS, &REQUESTED, &LIVE, &PEAK] {
        count.store(0, Ordering::Relaxed);
    }
    ENABLED.store(true, Ordering::SeqCst);
}
pub fn stop() -> String {
    ENABLED.store(false, Ordering::SeqCst);
    assert!(
        LIVE.load(Ordering::Relaxed) <= isize::MAX as usize
            && PEAK.load(Ordering::Relaxed) <= isize::MAX as usize,
        "allocation interval contains an unmatched pre-existing deallocation"
    );
    format!(
        "{{\"calls\":{},\"requested_bytes\":{},\"peak_extra_live_bytes\":{},\"live_at_stop\":{}}}",
        CALLS.load(Ordering::Relaxed),
        REQUESTED.load(Ordering::Relaxed),
        PEAK.load(Ordering::Relaxed),
        LIVE.load(Ordering::Relaxed)
    )
}
