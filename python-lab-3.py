"""
Python Interview & Puzzle Gems (100+ More Tricky Features)
----------------------------------------------------------
Each entry includes a concise problem statement and a minimal solution.
This script is tailored for interview and puzzle prep.
"""

print("--- Python Hidden + Interview Gems Start ---\n")

# 61. Problem: Reversing a string with slicing
s = 'Python'
reversed_s = s[::-1]
print('61.', reversed_s)

# 62. Problem: Use functools.partial to pre-fill arguments
from functools import partial
def multiply(a, b): return a * b
double = partial(multiply, 2)
print('62.', double(5))

# 63. Problem: Unzip a list of tuples
pairs = [(1, 'a'), (2, 'b'), (3, 'c')]
numbers, letters = zip(*pairs)
print('63.', numbers, letters)

# 64. Problem: Swap numbers using XOR (no temp)
a, b = 5, 9
a = a ^ b
b = b ^ a
a = a ^ b
print('64.', a, b)

# 65. Problem: Check for an empty list
lst = []
print('65.', 'Empty' if not lst else 'Has items')

# 66. Problem: Efficient string join
words = ['Python', 'is', 'fun']
print('66.', ' '.join(words))

# 67. Problem: Enumerate with custom start
lst = ['e', 'f', 'g']
for idx, val in enumerate(lst, 5):
    print('67.', idx, val)

# 68. Problem: Case-insensitive sorting
lst = ['a', 'B', 'c', 'D']
sorted_lst = sorted(lst, key=lambda x: x.lower())
print('68.', sorted_lst)

# 69. Problem: Generate a cartesian product
import itertools
for combo in itertools.product('AB', '123'):
    print('69.', combo)

# 70. Problem: For-else, run word only if no break
for i in range(5):
    if i == 10:
        break
else:
    print('70. Loop completed without break')

# 71. Problem: Chained assignment works right-to-left
a = b = c = 10
print('71.', a, b, c)

# 72. Problem: Flatten nested lists with itertools.chain
import itertools
nested = [[1,2], [3,4]]
flat = list(itertools.chain.from_iterable(nested))
print('72.', flat)

# 73. Problem: Find the most frequent element
lst = [1, 2, 2, 3, 1, 1]
print('73.', max(set(lst), key=lst.count))  # Output: 1

# 74. Problem: Use _ in unpacking to ignore items
x, _, y = (10, 99, 20)
print('74.', x, y)

# 75. Problem: Negative index to slice the last two
l = [7, 8, 9, 20]
print('75.', l[-2:])

# 76. Problem: List multiplication for shallow copy
l = [0] * 5
print('76.', l)

# 77. Problem: List comprehensions for indices
odd_indices = [i for i, v in enumerate(range(8)) if v % 2]
print('77.', odd_indices)

# 78. Problem: Nested list comp for matrix transpose
mat = [[1,2,3],[4,5,6]]
transpose = [[row[i] for row in mat] for i in range(len(mat[0]))]
print('78.', transpose)

# 79. Problem: Use any/all for quick checks
data = [0, '', None, 10]
print('79.', 'Any true:', any(data), 'All true:', all(data))

# 80. Problem: Test for unique list elements
lst = [1, 2, 2, 3]
print('80.', 'Unique' if len(lst) == len(set(lst)) else 'Non-unique')

# 81. Problem: Unpack with starred variable to grab the rest
first, *rest = [1, 2, 3, 4, 5]
print('81.', first, rest)

# 82. Problem: Use os.path.join for portable paths
import os
print('82.', os.path.join('folder', 'file.txt'))

# 83. Problem: Safe dict get with default value
d = {'a': 100}
print('83.', d.get('b', 'Not found'))

# 84. Problem: Remove duplicates from sequence, preserve order
seen = set()
lst = [2, 2, 3, 1]
unique = [x for x in lst if not (x in seen or seen.add(x))]
print('84.', unique)

# 85. Problem: Merging dicts with {**a, **b}
a, b = {'x':1}, {'y':2}
merged = {**a, **b}
print('85.', merged)

# 86. Problem: Merge two lists element-wise with zip
print('86.', [a+b for a, b in zip([1,2,3], [4,5,6])])

# 87. Problem: Using format string with named placeholders
print('87.', '{name} uses {lang}'.format(name='Sam', lang='Python'))

# 88. Problem: Repr vs str difference
class C: pass
o = C()
print('88.', str(o), repr(o))

# 89. Problem: Find index of max value
x = [2, 8, 3]
print('89.', x.index(max(x)))

# 90. Problem: Print without newline
print('90. ', end=''); print('Next!')

# 91. Problem: Contextlib for custom context managers
from contextlib import contextmanager
@contextmanager
def demo_cm():
    print("Enter context")
    yield
    print("Exit context")
with demo_cm():
    print('91. In context')

# 92. Problem: Value unpacking for function args
def add(a, b): return a + b
tup = (3, 5)
print('92.', add(*tup))

# 93. Problem: Dict sorting by values
d = {'a':3, 'b':1, 'c':2}
print('93.', sorted(d, key=d.get))

# 94. Problem: Dict comprehensions for reversal
d = {'a': 1, 'b': 2}
inv = {v: k for k, v in d.items()}
print('94.', inv)

# 95. Problem: Count chars in string with collections.Counter
from collections import Counter
print('95.', Counter('lynx'))

# 96. Problem: Safely remove item from list (if exists)
l = [1,2,3]
if 2 in l:
    l.remove(2)
print('96.', l)

# 97. Problem: Chained comparison operators
x = 5
print('97.', 1 < x < 10)

# 98. Problem: Inline lambda sort by absolute value
lst = [-4, -2, 1, 0]
print('98.', sorted(lst, key=lambda x: abs(x)))

# 99. Problem: Print list as comma string with unpacking
l = [4,3,2,1]
print('99.', *l, sep=',')

# 100. Problem: Conditional expressions (ternary)
x = 4
print('100.', 'Even' if x%2==0 else 'Odd')

# 101. Problem: Unique combinations using set comprehension
nums = [1, 2, 3, 2]
unique = {x for x in nums}
print('101.', unique)

# 102. Problem: map with None yields zipped tuples
print('102.', list(map(lambda *x: x, [1,2,3], [4,5,6])))

# 103. Problem: Use of assert for precondition checks
assert 1 + 1 == 2
print('103. assert passed')

# 104. Problem: Test if a value is a number (int, float)
def is_number(val): return isinstance(val, (int, float))
print('104.', is_number(3.14), is_number('a'))

# 105. Problem: Use hasattr/getattr/setattr for dynamic members
class Dummy: pass
d = Dummy()
setattr(d, 'foo', 123)
print('105.', getattr(d, 'foo'))

# 106. Problem: sum over boolean list to count True
lst = [True, False, True]
print('106.', sum(lst))

# 107. Problem: Quickly flatten list-of-lists with sum
lsts = [[1,2],[3,4]]
print('107.', sum(lsts, []))

# 108. Problem: Remove None from list with filter
l = [1, None, 2, None, 3]
print('108.', list(filter(None, l)))

# 109. Problem: is operator vs == for identity vs equality
a = [1]; b = a
print('109.', a is b, a == b)

# 110. Problem: is not for not identity
x = y = []
z = []
print('110.', x is not z)

# 111. Problem: Use reversed() for efficient reverse iteration
for x in reversed([1,2,3]):
    print('111.', x)

# 112. Problem: Math tricks - division to float by default
print('112.', 1/2, type(1/2))

# 113. Problem: Integer division with //
print('113.', 7//2)

# 114. Problem: modulo on negative numbers
print('114.', -7 % 3)

# 115. Problem: Named arguments can be given in any order
def foo(a, b): return a+b
print('115.', foo(b=3, a=2))

# 116. Problem: Dict/Set comprehensions with conditions
print('116.', {x for x in range(6) if x%2})

# 117. Problem: all uses lazy evaluation; early exit
print('117.', all([0, 1/0, 2]))

# 118. Problem: zip_longest from itertools for uneven lists
from itertools import zip_longest
a = [1,2]
b = [3,4,5]
print('118.', list(zip_longest(a, b)))

# 119. Problem: Countlines, words, chars in text with split and len
line = "The quick brown fox"
print('119.', len(line.split()), len(line))

# 120. Problem: Use bisect for binary search
import bisect
lst = [1,3,5,7]
bisect.insort(lst, 4)
print('120.', lst)

# 121. Problem: Quick find min/max with custom key
words = ['Python', 'java', 'C++']
print('121.', max(words, key=len), min(words, key=str.lower))

# 122. Problem: all() returns True for empty, any() returns False
print('122.', all([]), any([]))

# 123. Problem: Dict view objects update with dict
d = {'a':1}
keys = d.keys()
d['b'] = 2
print('123.', list(keys))

# 124. Problem: Sorting tuples sorts by first, then second
l = [(2,3), (1,10), (2,1)]
print('124.', sorted(l))

# 125. Problem: Use *args, **kwargs for flexible functions
def foo(*args, **kwargs): print('125.', args, kwargs)
foo(10, 20, x='y')

# 126. Problem: Use super() to call parent constructor/method
class A: 
    def hello(self): print('hi')
class B(A): 
    def hello(self): super().hello(); print('bye')
B().hello()

# 127. Problem: Chained comparisons with == and !=
print('127.', 0 == 0 != 1 < 2 == 2)

# 128. Problem: dict.pop() returns and removes key
d = {'a':1}
val = d.pop('a')
print('128.', val, d)

# 129. Problem: Use operator module for cleaner lambdas
import operator
l = [4, 2, 9]
print('129.', sorted(l, key=operator.neg))

# 130. Problem: Using set operations (|, &, ^, -) on sets
a = {1,2,3}; b = {2,3,4}
print('130.', a|b, a&b, a^b, a-b)

# 131. Problem: Reading input as int list
# input_line = input()
# nums = list(map(int, input_line.split()))
print('131. Input trick: list(map(int, input().split()))')

# 132. Problem: All elements the same? Convert to set, length 1
arr = [5,5,5]
print('132.', len(set(arr)) == 1)

# 133. Problem: Swap values of dict keys and values
d = {'x':1, 'y':2}
swapped = dict(map(reversed, d.items()))
print('133.', swapped)

# 134. Problem: Compare two lists for same items ignoring order
l1, l2 = [1,2,3], [3,2,1]
print('134.', set(l1) == set(l2))

# 135. Problem: Remove duplicates and sort
lst = [3,1,2,2]
print('135.', sorted(set(lst)))

# 136. Problem: Print variable names and values using locals()
a = 10
print('136.', 'a is', locals()['a'])

# 137. Problem: List comprehension with function
def f(x): return x*2
print('137.', [f(i) for i in range(3)])

# 138. Problem: Filter list with lambda
print('138.', list(filter(lambda x: x>1, [0,1,2,3])))

# 139. Problem: Chain function calls (method chaining)
print('139.', '  abc '.strip().upper())

# 140. Problem: Unpack a string into variables
x, y, z = 'abc'
print('140.', x, y, z)

# 141. Problem: Splat operator to expand generator in function call
def sum_(*args): return sum(args)
g = (i for i in range(5))
print('141.', sum_(*g))

# 142. Problem: Count truthy values with sum(map(bool, ...))
data = [0, "", 1, "Hi", [], {}]
print('142.', sum(map(bool, data)))

# 143. Problem: Slicing strings with steps
s = 'abcdefg'
print('143.', s[::2])

# 144. Problem: Remove whitespace efficiently
s = '  abc def   '
print('144.', ''.join(s.split()))

# 145. Problem: List multiplication creates references (not copies)
l = [[0]] * 3
l[0][0] = 9
print('145.', l)

# 146. Problem: object() gives unique singleton objects
x, y = object(), object()
print('146.', x is y)

# 147. Problem: Detect palindrome with slicing
w = 'madam'
print('147.', w == w[::-1])

# 148. Problem: Use or for default value
x = '' or 'default'
print('148.', x)

# 149. Problem: Using zip to pair items, then unpack pairs as lists
a = [1,2,3]
b = [4,5,6]
pairs = list(zip(a,b))
aa, bb = zip(*pairs)
print('149.', aa, bb)

# 150. Problem: Quick fibonacci with tuple unpacking
a, b = 0, 1
for _ in range(5): a, b = b, a+b
print('150.', a)

# 151. Problem: Use __slots__ for memory-optimized classes
class S:
    __slots__ = ('x','y')
    def __init__(self,x,y): self.x, self.y = x,y
s = S(5,6)
print('151.', s.x, s.y)

# 152. Problem: dataclasses for automagic classes (3.7+)
from dataclasses import dataclass
@dataclass
class Point:
    x: int
    y: int
p = Point(2,3)
print('152.', p.x, p.y)

# 153. Problem: Enum for readable symbolic constants
from enum import Enum
class Color(Enum):
    RED = 1
    GREEN = 2
print('153.', Color.RED.name, Color.RED.value)

# 154. Problem: Use * for unpacking in function calls, assignments
def foo(a, b, c): return a+b+c
args = [1,2,3]
print('154.', foo(*args))

# 155. Problem: Use globals() for dynamic variable access
globals()['gg'] = 77
print('155.', gg)

# 156. Problem: Chain comparisons with custom objects
class V: 
    def __eq__(self, other): return True
v = V()
print('156.', v == v == v)

# 157. Problem: Chain of .replace() calls
print('157.', 'one two three'.replace(' ', '-').replace('o', 'O'))

# 158. Problem: Using nonlocal inside nested functions
def outer():
    counter = 0
    def inner():
        nonlocal counter
        counter += 1
        return counter
    return inner
f = outer()
print('158.', f(), f())

# 159. Problem: Use exec to run code from a string
exec('a=10')
print('159.', a)

# 160. Problem: Print all args of a callable with inspect
import inspect
def foo(a, b=1): pass
print('160.', inspect.getfullargspec(foo).args)

# 161. Problem: Infinite iterators with itertools.count
from itertools import count
c = count(10)
for _ in range(3):
    print('161.', next(c))

# 162. Problem: Using math.gcd for greatest common divisor
import math
print('162.', math.gcd(42,56))

# 163. Problem: Sequences are truthy if nonempty, False if empty
print('163.', bool([]), bool([1]))

# 164. Problem: Use dis module to disassemble python bytecode
import dis
def f(x): return x+1
dis.dis(f)

# 165. Problem: Use of __main__ idiom for script entry point
if __name__ == "__main__":
    print('165. __main__ block is working.')

# 166-170. (Bonus) Miscellaneous Quickies
print('166.', 'apple' < 'banana')  # String compare
print('167.', True + True == 2)    # True is 1
print('168.', [*range(5)])         # Expanding range to list
print('169.', chr(65), ord('A'))   # ASCII conversion
print('170.', format(12345, ',d')) # Number with commas

print("\n--- All 100+ Python tricky/interview features shown ---")
