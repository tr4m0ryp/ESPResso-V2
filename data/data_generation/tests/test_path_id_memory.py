"""
Tests for counter-based path ID generation in PreprocessingPathGenerator.

Verifies that path IDs are unique and that the implementation uses O(1)
memory (no growing set of previously issued IDs).
"""

import sys
import threading
from unittest.mock import MagicMock


def _make_generator():
    """Build a PreprocessingPathGenerator with all dependencies mocked out.

    The constructor calls format_for_prompt() and format_compact_for_prompt()
    on the processing data objects, so we mock those to return empty strings.
    It also instantiates a PromptBuilder, which we patch to avoid importing
    the full module tree.
    """
    # Lazy import so the test file can be collected even if the broader
    # package is not fully installed.
    from data.data_generation.layer_2.core.generator import PreprocessingPathGenerator

    config = MagicMock()
    config.paths_per_product = 5

    processing_steps_db = MagicMock()
    processing_steps_db.format_for_prompt.return_value = ""

    material_process_combos = MagicMock()
    material_process_combos.format_compact_for_prompt.return_value = ""

    api_client = MagicMock()

    gen = PreprocessingPathGenerator(
        config=config,
        processing_steps_db=processing_steps_db,
        material_process_combos=material_process_combos,
        api_client=api_client,
    )
    return gen


# -- Test 1: 1000 sequential IDs are all unique --

def test_sequential_ids_unique():
    gen = _make_generator()
    ids = []
    for _ in range(1000):
        with gen._id_lock:
            ids.append(gen._generate_unique_path_id())
    assert len(ids) == 1000
    assert len(set(ids)) == 1000, "Not all generated IDs were unique"


# -- Test 2: IDs follow the expected format --

def test_id_format():
    gen = _make_generator()
    with gen._id_lock:
        first = gen._generate_unique_path_id()
    assert first == "pp-000001", f"Expected pp-000001, got {first}"

    for _ in range(998):
        with gen._id_lock:
            gen._generate_unique_path_id()

    with gen._id_lock:
        thousandth = gen._generate_unique_path_id()
    assert thousandth == "pp-001000", f"Expected pp-001000, got {thousandth}"


# -- Test 3: O(1) memory -- no growing collection on the generator --

def test_no_growing_set():
    gen = _make_generator()

    # The old implementation stored IDs in _generated_path_ids (a set).
    # Verify that attribute no longer exists.
    assert not hasattr(gen, "_generated_path_ids"), (
        "_generated_path_ids set still exists on the generator; "
        "it should have been replaced by a counter"
    )

    # Generate 1000 IDs and confirm the only state that grew is _path_counter.
    for _ in range(1000):
        with gen._id_lock:
            gen._generate_unique_path_id()

    assert gen._path_counter == 1000

    # Measure actual object size: only the counter (an int) should store
    # state proportional to the number of IDs. An int in CPython is ~28 bytes
    # regardless of magnitude (up to ~2^30). A set of 1000 strings would be
    # well over 50 KB.
    counter_size = sys.getsizeof(gen._path_counter)
    assert counter_size < 100, (
        f"Counter size unexpectedly large: {counter_size} bytes"
    )


# -- Test 4: Thread safety -- concurrent generation produces no duplicates --

def test_thread_safety():
    gen = _make_generator()
    results = [None] * 200
    errors = []

    def worker(start, count):
        try:
            for i in range(count):
                with gen._id_lock:
                    results[start + i] = gen._generate_unique_path_id()
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(i * 50, 50))
        for i in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Thread errors: {errors}"
    assert len(set(results)) == 200, "Concurrent generation produced duplicates"


# -- Test 5: get_deduplication_stats reflects the counter --

def test_deduplication_stats():
    gen = _make_generator()

    stats = gen.get_deduplication_stats()
    assert stats["unique_path_ids"] == 0
    assert stats["total_generated"] == 0

    for _ in range(50):
        with gen._id_lock:
            gen._generate_unique_path_id()

    stats = gen.get_deduplication_stats()
    assert stats["unique_path_ids"] == 50


# -- Entry point for running directly --

if __name__ == "__main__":
    test_sequential_ids_unique()
    print("PASS: test_sequential_ids_unique")

    test_id_format()
    print("PASS: test_id_format")

    test_no_growing_set()
    print("PASS: test_no_growing_set")

    test_thread_safety()
    print("PASS: test_thread_safety")

    test_deduplication_stats()
    print("PASS: test_deduplication_stats")

    print("\nAll tests passed.")
