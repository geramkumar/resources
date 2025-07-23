"""
Ultra-Comprehensive Python 3 & OOP Concepts Review
-------------------------------------------------
This script covers every major area of Python (including list slicing and comprehensions in detail),
with rich samples for each topic, as a complete study and reference tool.
"""

# ================================
# 1. BASIC DATA TYPES & CONVERSIONS
# ================================

# Numbers
an_int = 42
a_float = 3.14
a_complex = 5 + 2j

# Booleans
flag_true = True
flag_false = False

# Strings
s = "Hello, Python3!"

# Bytes and bytearray
ba = b'Binary'
barr = bytearray([65, 66, 67])

# NoneType
nothing_here = None

# Type conversions
to_str = str(100)
to_int = int("123")
to_float = float("9.8")
to_bool = bool('')
to_complex = complex("1+2j")

# ============================
# 2. LISTS - WITH DETAILED SLICING AND COMPREHENSIONS
# ============================

# List creation and basic use
nums = [10, 20, 30, 40, 50, 60]
mixed = [1, 'a', 3.5, [4, 5]]

# Slicing basics
first_three = nums[0:3]           # [10, 20, 30]
from_second = nums[1:]            # [20, 30, 40, 50, 60]
last_two = nums[-2:]              # [50, 60]

# Advanced slicing
middle = nums[2:5]                # [30, 40, 50]
step_two = nums[::2]              # [10, 30, 50]
reverse_list = nums[::-1]         # [60, 50, 40, 30, 20, 10]
every_third = nums[::3]           # [10, 40]
first_to_third = nums[:3]         # [10, 20, 30]

# Slicing assignment
nums[1:3] = [99, 98]              # nums becomes [10, 99, 98, 40, 50, 60]

# Copying lists (various ways)
copy1 = nums[:]
copy2 = list(nums)
copy3 = nums.copy()

# List comprehensions - all possible patterns

# Basic - squares
squares = [x**2 for x in range(6)]           # [0, 1, 4, 9, 16, 25]

# With if (filtering)
even_squares = [x*x for x in range(10) if x%2==0]   # [0, 4, 16, 36, 64]

# Multiple for-loops
cartesian = [(a, b) for a in [1,2] for b in [3,4]]  # [(1,3), (1,4), (2,3), (2,4)]

# If-else in expression
odds_evens = ['even' if x%2==0 else 'odd' for x in range(6)]  # ['even', 'odd', ...]

# List of tuples
pairs = [(x, x**2) for x in range(5)]              # [(0,0), (1,1), ...]

# Nested comprehensions (flattening)
matrix = [[1,2],[3,4]]
flattened = [num for row in matrix for num in row]  # [1,2,3,4]

# Conditional comprehensions
greater_than_25 = [x for x in squares if x > 25]   # [36, 49, 64, 81, ...] for higher ranges

# All list methods (with sample usage)
sample = [44, 22, 22, 88, 66]
sample.append(77)
sample.extend([55,11])
sample.insert(1,123)
sample.remove(22)
vx = sample.pop()
count_of_22 = sample.count(22)
idx_66 = sample.index(66)
sample.sort()
sample.reverse()
copy_sample = sample.copy()
sample.clear()

# Traversal
for i, val in enumerate(['a','b','c']):
    print(f"List idx {i}: {val}")

# ============================
# 3. TUPLES
# ============================

tup = (1, 2, 2, 5)
count_of_2 = tup.count(2)
index_of_5 = tup.index(5)
# Tuple unpacking, including starred
a, b, *rest = tup   # a=1, b=2, rest=[2,5]

# ============================
# 4. SETS - ALL METHODS
# ============================

st = {1, 2, 3}
st.add(4)
st.update([5,6])
st.remove(2)
st.discard(10)      # No error if not present
unioned = st.union({7, 8, 1})
intersected = st.intersection({1,4,7})
difference = st.difference({1,6,9})
symmetric = st.symmetric_difference({1,7})
subset_check = st.issubset({1,3,4,5,6})
superset_check = st.issuperset({3,4})
disjoint_check = st.isdisjoint({9,10})
popped_set_val = st.pop()
st_copy = st.copy()
st.clear()
froz = frozenset([1,2,2])

# ============================
# 5. DICTIONARIES - EXAMPLES FOR ALL METHODS
# ============================

d = {'one': 1, 'two': 2}
d2 = dict(abc=123)
d3 = dict([('k', 9), ('l', 8)])
d4 = dict.fromkeys(['m','n'], 0)

# Access and mutation
d['three'] = 3
val_one = d.get('one', -1)
all_keys = list(d.keys())
all_values = list(d.values())
all_items = list(d.items())
d_copy = d.copy()
d.update({'four':4, 'one':10})
removed_val = d.pop('one')
removed_pair = d.popitem()
d.setdefault('five', 5)
d.clear()

# Dictionary traversal
for k in d_copy:
    print(f"Key: {k}, Value: {d_copy[k]}")
for k,v in d_copy.items():
    print(f"{k}=>{v}")

# Dictionary comprehensions
squared_dict = {x:x**2 for x in range(5)}
filtered_dict = {k:v for k,v in d_copy.items() if v>2}

# ============================
# 6. STRINGS - DETAILED SAMPLES
# ============================

str1 = "  Hello, Python!  "
s2 = str1.strip()           # Remove whitespace ends
s3 = str1.lstrip()
s4 = str1.rstrip()
s5 = str1.lower()
s6 = str1.upper()
s7 = str1.replace("Hello","Hi")
s8 = str1.split(",")
joined = ",".join(['1','2','3'])
pos = str1.find("Python")
cnt = str1.count("l")
is_digit = "123".isdigit()
is_alpha = "Word".isalpha()
zfilled = "42".zfill(5)
ljusted = "hi".ljust(6, '*')
rjusted = "hi".rjust(6, '-')
startswith = str1.startswith("  ")
endswith = str1.endswith("!  ")

# String slicing
sliced = str1[2:8]
reversed_str = str1[::-1]

# ============================
# 7. CONTROL FLOW
# ============================

val = -1
if val < 0:
    result = "Negative"
elif val == 0:
    result = "Zero"
else:
    result = "Positive"

# For with enumerate
lst = ['a','b','c']
for i, v in enumerate(lst):
    print(i,v)

# While with else
n = 3
while n:
    n -= 1
    if n==1:
        continue
    if n==0:
        break
else:
    print("Completed without break.")

tern_res = 'even' if n%2 == 0 else 'odd'

# ============================
# 8. FUNCTIONS (SIGNATURES, LAMBDA, CLOSURE, DECORATOR)
# ============================

def full_func(a, b=2, *args, kwonly=5, **kwargs):
    print(a, b, args, kwonly, kwargs)
full_func(8, 9, 10, 11, kwonly=20, z=22, k=33)

doubler = lambda x: x * 2

def power_func(pow):
    def inner(x):
        return x ** pow
    return inner

cube = power_func(3)
print(f"Cube of 2: {cube(2)}")

from functools import wraps
def trace(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        print(f"Calling {fn.__name__}")
        return fn(*args, **kwargs)
    return wrapper

@trace
def say_hello(name):
    print(f"Hello, {name}")

say_hello("World")

# ============================
# 9. CLASSES & OOP
# ============================

class Animal:
    kind = "Animalia"
    def __init__(self, name):
        self.name = name
    def speak(self):
        return f"{self.name} makes a sound"
    @classmethod
    def get_kind(cls):
        return cls.kind
    @staticmethod
    def static_info():
        return "All animals"
    def __str__(self):
        return f"Animal: {self.name}"
    def __repr__(self):
        return f"Animal({self.name})"

animal = Animal("Bobby")
print(animal.speak(), Animal.get_kind(), Animal.static_info(), str(animal), repr(animal))

# Subclass & override
class Dog(Animal):
    def speak(self):
        return f"{self.name} says woof!"

dog = Dog("Max")
print(dog.speak())

# Multiple inheritance, super(), MRO
class Climber: pass
class MountainDog(Dog, Climber): pass
print(MountainDog.mro())

# Polymorphism
for pet in [Animal("Joe"), Dog("Ruby")]:
    print(pet.speak())

# ============================
# 10. PROPERTY DECORATORS
# ============================

class Person:
    def __init__(self, age):
        self._age = age
    @property
    def age(self):
        return self._age
    @age.setter
    def age(self, val):
        if val<0: raise ValueError("No negative age")
        self._age = val

person = Person(20)
person.age = 25

# ============================
# 11. MAGIC METHODS
# ============================

class Point:
    def __init__(self, x, y):
        self.x,self.y = x,y
    def __str__(self):
        return f"({self.x},{self.y})"
    def __repr__(self):
        return f"Point({self.x},{self.y})"
    def __add__(self, other):
        return Point(self.x + other.x, self.y + other.y)
    def __eq__(self, other):
        return self.x == other.x and self.y == other.y
    def __len__(self):
        return 2

pt1 = Point(1,2)
pt2 = Point(2,3)
print(pt1+pt2, pt1==Point(1,2), len(pt1))

# ============================
# 12. ABSTRACT BASE CLASSES & MULTIPLE INHERITANCE
# ============================

from abc import ABC, abstractmethod

class Shape(ABC):
    @abstractmethod
    def area(self):
        pass

class Rectangle(Shape):
    def __init__(self,w,h):
        self.w = w; self.h = h
    def area(self):
        return self.w * self.h

class ColorMixin:
    color = "red"
class Square(Rectangle, ColorMixin):
    pass

sq = Square(3,3)
print(sq.area(), sq.color)

# ============================
# 13. GENERATORS & ITERATORS
# ============================

def gen(n):
    for i in range(n):
        yield i*i

for val in gen(4):
    print("Generator yields:", val)

class Range:
    def __init__(self, start, end): self.i, self.end = start, end
    def __iter__(self): return self
    def __next__(self):
        if self.i >= self.end: raise StopIteration
        val = self.i; self.i += 1; return val

for v in Range(2,5):
    print("Custom iterator:", v)

# ============================
# 14. EXCEPTION HANDLING & CUSTOM EXCEPTIONS
# ============================

try:
    1/0
except ZeroDivisionError:
    print("Division by zero!")
except Exception as e:
    print("Other error:", e)
else:
    print("No Errors")
finally:
    print("Cleanup always runs.")

class MyError(Exception): pass

try:
    raise MyError("Something went wrong")
except MyError as e:
    print("Caught:", e)

# ============================
# 15. COLLECTIONS, COMPREHENSION, UTILITIES
# ============================

from collections import namedtuple, Counter, defaultdict, deque

Pt = namedtuple('Pt', 'x y')
pt = Pt(1,2)

dd = defaultdict(list)
dd['a'].append(10)

ctr = Counter('banana')
dq = deque([1,2,3])
dq.append(4)
dq.appendleft(0)
dq.pop(); dq.popleft()

# Comprehensions
lst_comp = [x+1 for x in range(3)]
set_comp = {x**2 for x in range(5)}
dict_comp = {x:chr(65+x) for x in range(3)}

# ============================
# 16. FILE IO
# ============================

with open("eg.txt", "w", encoding="utf-8") as f:
    f.write("Hello file demo\n")

with open("eg.txt", "r", encoding="utf-8") as f:
    print("Read back:", f.read())

# ============================
# 17. TYPE HINTS & ANNOTATIONS
# ============================

from typing import List, Dict, Optional, Any, Union, Tuple

def mean(nums: List[float]) -> float:
    return sum(nums) / len(nums)

def search_dict(d: Dict[str, int], key: str) -> Optional[int]:
    return d.get(key)

# ============================
# 18. BUILT-IN FUNCTIONS & OPERATORS
# ============================

nums2 = [4, 7, 1]
print("len:", len(nums2), "max:", max(nums2), "min:", min(nums2),
      "sum:", sum(nums2), "any >5:", any(x > 5 for x in nums2),
      "all >0:", all(x > 0 for x in nums2))
pairs = list(zip([1,2], ['a','b']))
print("zipped:", pairs)
print("sorted desc:", sorted(nums2, reverse=True))
doubled = list(map(lambda x: x*2, nums2))
evens = list(filter(lambda x: x%2==0, nums2))
from functools import reduce
total = reduce(lambda x,y: x+y, nums2)

# ============================
# 19. PATTERN MATCHING (Python 3.10+)
# ============================

def check(val):
    match val:
        case 0:
            return "Zero"
        case [a, b]:
            return f"List: {a}, {b}"
        case {'x': x, 'y': y}:
            return f"Dict: {x},{y}"
        case str() as s:
            return f"String pattern: {s}"
        case _:
            return "Other"

print(check(0))
print(check([1,2]))
print(check({'x':5,'y':10}))
print(check("hi"))

# ============================
# 20. MAIN GUARD
# ============================

if __name__ == "__main__":
    print("\n=== Ultra-Comprehensive Python3 + OOP Review Completed ===")
