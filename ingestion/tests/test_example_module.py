import pytest
from example_module import (
    is_palindrome,
    add_numbers,
    get_max_value,
    normalize_string,
    divide,
    reverse_list,
    subtract,
    multiply,
    safe_divide,
    power,
    square,
    is_anagram
)

def test_is_palindrome():
    assert is_palindrome("racecar") is True
    assert is_palindrome("hello") is False
    assert is_palindrome("") is True
    assert is_palindrome("a") is True

def test_add_numbers():
    assert add_numbers(1, 2) == 3
    assert add_numbers(-1, 1) == 0
    assert add_numbers(0, 0) == 0

def test_get_max_value():
    assert get_max_value([1, 2, 3]) == 3
    assert get_max_value([-1, -2, -3]) == -1
    with pytest.raises(ValueError):
        get_max_value([])

def test_normalize_string():
    assert normalize_string("  Hello   World  ") == "hello world"
    assert normalize_string("TEST") == "test"
    assert normalize_string("  multiple   spaces  ") == "multiple spaces"

def test_divide():
    assert divide(10, 2) == 5
    assert divide(-10, 2) == -5
    with pytest.raises(ZeroDivisionError):
        divide(10, 0)

def test_reverse_list():
    assert reverse_list([1, 2, 3]) == [3, 2, 1]
    assert reverse_list([]) == []
    assert reverse_list(['a', 'b', 'c']) == ['c', 'b', 'a']

def test_subtract():
    assert subtract(10, 5) == 5
    assert subtract(0, 0) == 0
    assert subtract(-5, -5) == 0

def test_multiply():
    assert multiply(3, 4) == 12
    assert multiply(-1, 5) == -5
    assert multiply(0, 100) == 0

def test_safe_divide():
    assert safe_divide(10, 2) == 5
    assert safe_divide(-10, 2) == -5
    with pytest.raises(ValueError):
        safe_divide(10, 0)

def test_power():
    assert power(2, 3) == 8
    assert power(5, 0) == 1
    assert power(2, -1) == 0.5

def test_square():
    assert square(3) == 9
    assert square(-3) == 9
    assert square(0) == 0

def test_is_anagram():
    assert is_anagram("listen", "silent") is True
    assert is_anagram("hello", "world") is False
    assert is_anagram("Dormitory", "Dirty room") is True