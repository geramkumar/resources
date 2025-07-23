"""
Python Hidden Gems with Problem & Solution Format
------------------------------------------------
This script presents 58+ interesting Python features.
Each item has:
 - A problem statement
 - The pythonic solution demonstrating a "hidden gem" or tricky feature.
"""

print("--- Python Hidden Gems Start ---\n")

# 1. Problem: Assign and test a variable inside a condition
# Solution: Use walrus operator `:=` to assign while testing condition (Python 3.8+)
if (n := len([1, 2, 3, 4])) > 3:
    print("1. Walrus operator: length =", n)

# 2. Problem: Check if a number is between 3 and 10
# Solution: Chained comparisons let you do this elegantly
x = 7
print("2.", 3 < x <= 10)

# 3. Problem: Swap two variables without temp var
# Solution: Use tuple unpacking
a, b = 5, 8
a, b = b, a
print("3. Swapped a,b =", a, b)

# 4. Problem: Unpack list but keep "middle" slice in separate list
# Solution: Extended unpacking with *
first, *middle, last = [10, 20, 30, 40, 50]
print("4. Middle elements:", middle)

# 5. Problem: Assign multiple variables in one line
a, b, c = (1, 2, 3)
print("5.", a, b, c)

# 6. Problem: Ignore some unpacked values
x, _, y = (1, 99, 2)
print("6. Ignored middle value:", x, y)

# 7. Problem: You only need part of tuple elements in a loop
pairs = [(1, 2), (3, 4), (5, 6)]
for a, _ in pairs:
    print("7. First paired value:", a)

# 8. Problem: How to access last element of list
l = [100, 200, 300]
print("8. Last element of list:", l[-1])

# 9. Problem: Slice list safely beyond its length (no errors)
print("9. Slicing beyond list length:", l[1:10])

# 10. Problem: Insert or replace multiple elements in list slice
nums = [1, 2, 3, 4]
nums[1:3] = [8, 9, 10]
print("10. After slice assignment:", nums)

# 11. Problem: List comprehension with conditional expressions
flags = ["even" if x % 2 == 0 else "odd" for x in range(4)]
print("11. Flags:", flags)

# 12. Problem: Flatten a nested list
matrix = [[1,2], [3,4]]
flat = [num for row in matrix for num in row]
print("12. Flattened list:", flat)

# 13. Problem: Create sets and dicts using comprehension
print("13. Set comprehension:", {x for x in 'hello'})
print("13. Dict comprehension:", {x: ord(x) for x in 'ace'})

# 14. Problem: Run code only if a loop didn't break
for i in range(3):
    if i == 5:
        break
else:
    print("14. Loop completed without break")

# 15. Problem: Use else with while (runs if no break)
n = 0
while n < 3:
    n += 1
else:
    print("15. While loop else runs")

# 16. Problem: Iterate two lists in parallel
a, b = [1, 2, 3], [4, 5, 6]
for x, y in zip(a, b):
    print("16. Sum of pairs:", x + y)

# 17. Problem: Get index with starting point
for idx, val in enumerate('abc', 1):
    print("17.", idx, val)

# 18. Problem: Check if all or any are true in a list
vals = [1, 0, 2]
print("18. all:", all(vals), "any:", any(vals))

# 19. Problem: Reverse a list using slicing
lst = [1, 2, 3, 4, 5]
print("19. Reversed list:", lst[::-1])

# 20. Problem: Repeat a string and join with delimiter
print("20. Repeat:", "ab" * 3)
print("20. Join:", "-".join(['1', '2', '3']))

# 21. Problem: Throwaway variables in loops
for _ in range(2):
    print("21. Underscore used as throwaway")

# 22. REPL trick: _ holds last result (interactive sessions only)

# 23. Problem: Assign multiple vars to same value simultaneously
a = b = c = 42
print("23. Multiple assignment:", a, b, c)

# 24. Problem: Modify variable in inner function from outer scope
def counter():
    count = 0
    def inc():
        nonlocal count
        count += 1
        return count
    return inc
inc_fn = counter()
print("24. Nonlocal counter:", inc_fn(), inc_fn())

# 25. Problem: Lazy computations needed
gen = (x * x for x in range(3))
print("25. Generator expression:", list(gen))

# 26. Problem: Generator delegation
def subgen():
    yield 1
    yield 2
def main_gen():
    yield 0
    yield from subgen()
print("26. Generator delegation:", list(main_gen()))

# 27. Problem: Safely retrieve next element from iterator or default
it = iter([1])
print("27.", next(it, 99))
print("27.", next(it, 99))

# 28. Problem: Simulate switch-case feature
def switch(val):
    return {1: "one", 2: "two"}.get(val, "other")
print("28. Switch dict:", switch(2))

# 29. Problem: Safe dict get and setdefault to handle missing keys
d = {'a': 10}
print("29. dict.get missing key:", d.get('b'))
d.setdefault('b', 99)
print("29. dict.setdefault added:", d['b'])

# 30. Problem: Count occurrences in iterable
from collections import Counter
print("30. Counter:", Counter("banana"))

# 31. Problem: Provide default factory for missing dict keys
from collections import defaultdict
dd = defaultdict(list)
dd['x'].append(7)
print("31. defaultdict:", dd)

# 32. Problem: Lightweight immutable classes
from collections import namedtuple
Pt = namedtuple('Pt', 'x y')
pt = Pt(3,4)
print("32. namedtuple:", pt.x, pt.y)

# 33. Problem: Efficient append/pop both ends
from collections import deque
dq = deque([1, 2, 3])
dq.appendleft(0)
dq.append(4)
print("33. deque:", dq)
dq.popleft()
dq.pop()
print("33. deque after pops:", dq)

# 34. Problem: Manipulating paths with pathlib instead of os.path
from pathlib import Path
cwd = Path('.').resolve()
print("34. cwd:", cwd)

# 35. Problem: Quickly print var and value (debug)
val = 13
print(f"35. Debug print: {val=}")

# 36. Problem: Readable large numbers
big = 10_000_000
print("36. underscore in numbers:", big)

# 37. Problem: Use special float constants
print("37. Infinity and NaN:", float('inf'), float('-inf'), float('nan'))

# 38. Problem: Chain string methods
s = "  hello python "
result = s.strip().upper().replace(" ", "-")
print("38. Method chaining:", result)

# 39. Problem: Ignore specific exceptions concisely
from contextlib import suppress
with suppress(ValueError):
    int("not a number")

print("39. suppress() used to ignore errors")

# 40. Problem: Use Ellipsis (...) as placeholder
def todo(): ...
print("40. Ellipsis is a valid object:", todo())

# 41. Problem: Dynamically add or get attributes
class Foo: pass
foo = Foo()
setattr(foo, "bar", 123)
print("41. Dynamic attr:", getattr(foo, "bar"))

# 42. Problem: Iterate backwards
seq = [10, 20, 30]
print("42. reversed():", list(reversed(seq)))

# 43. Problem: Case-insensitive sorted
names = ["Ada", "leo", "bob"]
print("43. Case-insensitive sort:", sorted(names, key=str.lower))

# 44. Problem: Sort dict / Counter by value
c = Counter('abracadabra')
print("44. Sorted by count:", sorted(c.items(), key=lambda kv: kv[1], reverse=True))

# 45. Problem: Inspect object attributes quickly
print("45. str has upper?", 'upper' in dir(str))

# 46. Problem: Write readable multi-line expressions with parentheses
xsum = (
    1
    + 2
    + 3
    + 4
)
print("46. Parenthesized sum:", xsum)

# 47. Problem: Customize dict missing key behavior
class MagicDict(dict):
    def __missing__(self, key):
        return 42
md = MagicDict(a=1)
print("47. __missing__:", md['b'])

# 48. Problem: Explicitly chain exceptions
try:
    raise ValueError("My error") from KeyError("Inner cause")
except Exception as e:
    print("48. Exception chain:", repr(e.__cause__))

# 49. Problem: Use positional-only and keyword-only args (Python 3.8+)
def demo_args(a, /, b, *, c):
    print("49. Args:", a, b, c)
demo_args(1, 2, c=3)

# 50. Problem: Use objects as dict keys
class A: pass
a1, a2 = A(), A()
obj_dict = {a1: 'x', a2: 'y'}
print("50. Different object keys:", obj_dict[a1], obj_dict[a2])

# 51. Problem: Debug with asserts
assert 2 + 2 == 4, "Math is broken!"

# 52. Problem: Quick access to Python's philosophy
import this  # prints Zen of Python monkey-patched on import

# 53. Problem: Guard script execution
if __name__ == "__main__":
    print("53. __main__ guard works!")

# 54. Problem: Insert breakpoints easily (Python 3.7+)
# breakpoint()  # Uncomment to start debugger here

# 55. (Interactive) REPL underscore _ holds last result

# 56. Problem: Get documentation quickly
# help(str)  # Uncomment to launch help system

# 57. Problem: Infinite counting with itertools + slicing
import itertools
print("57. Counting slice:", list(itertools.islice(itertools.count(10), 5)))

# 58. Problem: Safely open file with context manager
with open("gem.txt", "w", encoding="utf-8") as f:
    f.write("58. Context manager hides open/close\n")

print("58. File written safely using with statement")

print("\n--- All 58+ Python gems shown ---")
