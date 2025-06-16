```python
import pytest
from arkhamanalytics.example_module import (
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
    assert is_palindrome("A") is True
    assert is_palindrome("aa") is True
    assert is_palindrome("ab") is False

def test_add_numbers():
    assert add_numbers(1, 2) == 3
    assert add_numbers(-1, 1) == 0
    assert add_numbers(0, 0) == 0
    assert add_numbers(100, 200) == 300

def test_get_max_value():
    assert get_max_value([1, 2, 3, 4, 5]) == 5
    assert get_max_value([-1, -2, -3, -4, -5]) == -1
    assert get_max_value([5]) == 5
    with pytest.raises(ValueError):
        get_max_value([])

def test_normalize_string():
    assert normalize_string("  Hello World  ") == "hello world"
    assert normalize_string("HELLO") == "hello"
    assert normalize_string("  multiple   spaces  ") == "multiple spaces"
    assert normalize_string("") == ""

def test_divide():
    assert divide(10, 2) == 5.0
    assert divide(-10, 2) == -5.0
    assert divide(10, -2) == -5.0
    with pytest.raises(ZeroDivisionError):
        divide(10, 0)

def test_reverse_list():
    assert reverse_list([1, 2, 3]) == [3, 2, 1]
    assert reverse_list([]) == []
    assert reverse_list([1]) == [1]
    assert reverse_list([1, 2, 3, 4, 5]) == [5, 4, 3, 2, 1]

def test_subtract():
    assert subtract(10, 5) == 5
    assert subtract(5, 10) == -5
    assert subtract(0, 0) == 0
    assert subtract(-5, -5) == 0

def test_multiply():
    assert multiply(2, 3) == 6
    assert multiply(-2, 3) == -6
    assert multiply(0, 5) == 0
    assert multiply(5, 0) == 0

def test_safe_divide():
    assert safe_divide(10, 2) == 5.0
    assert safe_divide(-10, 2) == -5.0
    assert safe_divide(10, -2) == -5.0
    with pytest.raises(ValueError):
        safe_divide(10, 0)

def test_power():
    assert power(2, 3) == 8
    assert power(2, 0) == 1
    assert power(2, -1) == 0.5
    assert power(0, 5) == 0

def test_square():
    assert square(2) == 4
    assert square(-2) == 4
    assert square(0) == 0
    assert square(1.5) == 2.25

def test_is_anagram():
    assert is_anagram("listen", "silent") is True
    assert is_anagram("hello", "world") is False
    assert is_anagram("evil", "vile") is True
    assert is_anagram("a", "a") is True
    assert is_anagram("a", "b") is False
```