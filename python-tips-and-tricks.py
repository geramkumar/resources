"""
100+ Python Tricky, Interesting & Hidden Tips and Tricks
-------------------------------------------------------
Each item includes description, sample code, and printed output to demonstrate behavior.
Ideal for senior developers refreshing knowledge or interview prep.

Date: Wednesday, July 23, 2025, 8:17 AM IST
"""

print("--- Python Tips & Tricks Demo Start ---\n")

# 1. Walrus operator (:=) assigns inside expressions (Python 3.8+)
if (n := len([1, 2, 3, 4])) > 3:
    print(f"1. Walrus operator assigns and tests: n={n}")

# 2. Chained comparisons
x = 7
print(f"2. Chained comparisons (3 < x <= 10): {3 < x <= 10}")

# 3. Swap variables without temp
a, b = 5, 8
a, b = b, a
print("3. Swapped a, b:", a, b)

# 4. Extended unpacking with *
first, *middle, last = [10,20,30,40,50]
print("4. Extended unpacking: middle =", middle)

# 5. Multiple assignment
a, b, c = (1, 2, 3)
print("5. Multiple assignment:", a, b, c)

# 6. Ignore unpacked values with _
x, _, y = (1, 99, 2)
print("6. Ignored middle value:", x, y)

# 7. Unpack subset in loop
pairs=[(1,2),(3,4),(5,6)]
for a, _ in pairs:
    print("7. Loop unpack first val:", a)

# 8. Negative indices access from end
l = [100, 200, 300]
print("8. Last element l[-1]:", l[-1])

# 9. Slice beyond list length is safe
print("9. Slice l[1:10]:", l[1:10])

# 10. Slice assignment in list inserts/replaces
nums = [1,2,3,4]
nums[1:3] = [8,9,10]
print("10. Slice assignment:", nums)

# 11. List comprehension with inline if-else
flags = ["even" if x % 2==0 else "odd" for x in range(4)]
print("11. List comprehension flags:", flags)

# 12. Nested list comprehension to flatten matrix
matrix = [[1,2],[3,4]]
flat = [num for row in matrix for num in row]
print("12. Flattened list:", flat)

# 13. Set and dict comprehensions
print("13. Set comprehension:", {x for x in "hello"})
print("13. Dict comprehension:", {x: ord(x) for x in "ace"})

# 14. For-else: else runs only if no break
for i in range(3):
    if i == 5:
        break
else:
    print("14. For-else: loop finished without break")

# 15. While-else runs if no break
n = 0
while n < 3:
    n += 1
else:
    print("15. While-else triggered")

# 16. Parallel iteration with zip
a, b = [1,2,3], [4,5,6]
for x,y in zip(a,b):
    print("16. Sum pairs:", x+y)

# 17. Enumerate with customized start
lst = ['a','b','c']
for idx, val in enumerate(lst, 1):
    print("17. Enumerate start=1:", idx, val)

# 18. Any and All builtins
vals = [1, 0, 2]
print("18. all(vals):", all(vals), "any(vals):", any(vals))

# 19. Reverse list slice [::-1]
lst = [1,2,3,4,5]
print("19. Reverse with slice:", lst[::-1])

# 20. Repeat strings and join iterables
print("20. 'ab'*3:", "ab"*3)
print("20. '-'.join(['1','2','3']):", "-".join(['1','2','3']))

# 21. Use underscore (_) as throwaway variable
for _ in range(2):
    print("21. Throwaway var _ in loop")

# 22. REPL _ holds last expression (try in interactive shell)

# 23. Chain assignments right-to-left
a = b = c = 42
print("23. Chain assignment:", a, b, c)

# 24. Closures with nonlocal
def counter():
    count = 0
    def inc():
        nonlocal count
        count += 1
        return count
    return inc
inc_fn = counter()
print("24. Closure count:", inc_fn(), inc_fn())

# 25. Generator expression (lazy evaluation)
gen = (x*x for x in range(3))
print("25. Generator values:", list(gen))

# 26. Yield from delegation
def subgen():
    yield 1
    yield 2
def main_gen():
    yield 0
    yield from subgen()
print("26. Yield from delegation:", list(main_gen()))

# 27. next() with default avoids StopIteration
it = iter([1])
print("27. next(it, default):", next(it, 99), next(it, 99))

# 28. Simulate switch case with dict.get
def switch(val):
    return {1:"one", 2:"two"}.get(val, "other")
print("28. Simulated switch:", switch(2), switch(9))

# 29. dict.get and setdefault for safe/missing keys
d = {'a':10}
print("29. dict.get missing:", d.get('b'))
d.setdefault('b',99)
print("29. dict.setdefault added:", d['b'])

# 30. Count elements with collections.Counter
from collections import Counter
print("30. Counter count:", Counter("banana"))

# 31. defaultdict for automatic defaults on missing keys
from collections import defaultdict
dd = defaultdict(list)
dd['x'].append(7)
print("31. defaultdict result:", dd)

# 32. namedtuple immutables with field names
from collections import namedtuple
Pt = namedtuple('Pt', 'x y')
pt = Pt(3,4)
print("32. namedtuple fields:", pt.x, pt.y)

# 33. deque fast append/pop on both ends
from collections import deque
dq = deque([1,2,3])
dq.appendleft(0)
dq.append(4)
print("33. deque after append:", dq)
dq.popleft()
dq.pop()
print("33. deque after pop:", dq)

# 34. pathlib object-oriented file paths
from pathlib import Path
cwd = Path('.').resolve()
print("34. Current working dir:", cwd)

# 35. f-string debug = syntax (Python 3.8+)
val = 13
print(f"35. Debug print val={val}")

# 36. Underscores in numeric literals for readability
big = 10_000_000
print("36. Big number:", big)

# 37. Special float values: inf, -inf, nan
print("37. float special values:", float('inf'), float('-inf'), float('nan'))

# 38. Method chaining on strings
s = "  hello python "
result = s.strip().upper().replace(' ', '-')
print("38. Method chaining result:", result)

# 39. contextlib.suppress to ignore exceptions
from contextlib import suppress
with suppress(ValueError):
    int("not an int")
print("39. suppress used to ignore ValueError")

# 40. Ellipsis ... as a valid singleton
def todo(): ...
print("40. Ellipsis object:", todo())

# 41. Dynamic attributes with setattr/getattr
class Foo: pass
foo = Foo()
setattr(foo, 'bar', 123)
print("41. Dynamic attribute bar:", getattr(foo, 'bar'))

# 42. reversed() iterator
seq = [10, 20, 30]
print("42. reversed:", list(reversed(seq)))

# 43. Case-insensitive sort with key=str.lower
names = ['Ada', 'leo', 'bob']
print("43. Case-insensitive sort:", sorted(names, key=str.lower))

# 44. Sort dict items by value descending
c = Counter('abracadabra')
sorted_items = sorted(c.items(), key=lambda kv: kv[1], reverse=True)
print("44. Sorted counter items:", sorted_items)

# 45. dir() to inspect object methods/attrs
print("45. 'upper' in str dir:", 'upper' in dir(str))

# 46. Parentheses for multi-line expression readability
xsum = (1 + 2 + 3 + 4)
print("46. Parenthesized sum:", xsum)

# 47. dict subclass __missing__ method
class MagicDict(dict):
    def __missing__(self, key):
        return 42
md = MagicDict(a=1)
print("47. Missing key returns:", md['b'])

# 48. Exception chaining with raise...from
try:
    raise ValueError("My error") from KeyError("Inner")
except Exception as e:
    print("48. Exception cause:", repr(e.__cause__))

# 49. Positional-only and keyword-only arg notation (Python 3.8+)
def demo_args(a, /, b, *, c):
    print("49. Args:", a,b,c)
demo_args(1, 2, c=3)

# 50. Using instances as dict keys (object identity
