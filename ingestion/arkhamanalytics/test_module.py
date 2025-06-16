def is_palindrome(text: str) -> bool:
    """Check if a string is a palindrome."""
    return text == text[::-1]


def add_numbers(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


def get_max_value(numbers: list[int]) -> int:
    """Return the maximum value in a list."""
    if not numbers:
        raise ValueError("List cannot be empty")
    return max(numbers)


def normalize_string(text: str) -> str:
    """Strip, lowercase, and collapse multiple spaces."""
    return " ".join(text.strip().lower().split())


def divide(a: float, b: float) -> float:
    """Divide two numbers, raising an error on division by zero."""
    if b == 0:
        raise ZeroDivisionError("Division by zero is not allowed.")
    return a / b


def reverse_list(items: list) -> list:
    """Return the reversed list."""
    return items[::-1]

def subtract(a: int, b: int) -> int:
    return a - b

def multiply(a: int, b: int) -> int:
    return a * b

def safe_divide(a: float, b: float) -> float:
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b

def power(base: float, exp: int) -> float:
    return base ** exp

def square(x: float) -> float:
    return x * x

def is_anagram(word1: str, word2: str) -> bool:
    """Check if two strings are anagrams of each other."""
    return sorted(word1.lower()) == sorted(word2.lower())

#test
#test1
#test3
#test4
