To test the functions in the provided module, we can write a series of unit tests using the `pytest` framework. Below is a comprehensive set of tests for each function:

```python
import pytest

# Tests for is_palindrome
def test_is_palindrome_true():
    assert is_palindrome("racecar") is True

def test_is_palindrome_false():
    assert is_palindrome("hello") is False

def test_is_palindrome_empty_string():
    assert is_palindrome("") is True

def test_is_palindrome_single_character():
    assert is_palindrome("a") is True

# Tests for add_numbers
def test_add_numbers():
    assert add_numbers(2, 3) == 5
    assert add_numbers(-1, 1) == 0
    assert add_numbers(0, 0) == 0

# Tests for get_max_value
def test_get_max_value():
    assert get_max_value([1, 2, 3, 4, 5]) == 5
    assert get_max_value([-1, -2, -3]) == -1

def test_get_max_value_empty_list():
    with pytest.raises(ValueError):
        get_max_value([])

# Tests for normalize_string
def test_normalize_string():
    assert normalize_string("  Hello   World  ") == "hello world"
    assert normalize_string("TEST") == "test"
    assert normalize_string("  multiple   spaces  ") == "multiple spaces"

# Tests for divide
def test_divide():
    assert divide(10, 2) == 5.0
    assert divide(-10, 2) == -5.0

def test_divide_by_zero():
    with pytest.raises(ZeroDivisionError):
        divide(10, 0)

# Tests for reverse_list
def test_reverse_list():
    assert reverse_list([1, 2, 3]) == [3, 2, 1]
    assert reverse_list([]) == []
    assert reverse_list([1]) == [1]

# Tests for subtract
def test_subtract():
    assert subtract(10, 5) == 5
    assert subtract(0, 0) == 0
    assert subtract(-5, -5) == 0

# Tests for multiply
def test_multiply():
    assert multiply(3, 4) == 12
    assert multiply(-1, 5) == -5
    assert multiply(0, 100) == 0

# Tests for safe_divide
def test_safe_divide():
    assert safe_divide(10, 2) == 5.0
    assert safe_divide(-10, 2) == -5.0

def test_safe_divide_by_zero():
    with pytest.raises(ValueError):
        safe_divide(10, 0)

# Tests for power
def test_power():
    assert power(2, 3) == 8
    assert power(5, 0) == 1
    assert power(2, -2) == 0.25

# Tests for square
def test_square():
    assert square(3) == 9
    assert square(-3) == 9
    assert square(0) == 0

# Tests for is_anagram
def test_is_anagram_true():
    assert is_anagram("listen", "silent") is True
    assert is_anagram("evil", "vile") is True

def test_is_anagram_false():
    assert is_anagram("hello", "world") is False
    assert is_anagram("test", "sett") is False

def test_is_anagram_case_insensitive():
    assert is_anagram("Listen", "Silent") is True
```

These tests cover various scenarios for each function, including edge cases like empty inputs and error conditions. You can run these tests using `pytest` to ensure that the functions behave as expected.