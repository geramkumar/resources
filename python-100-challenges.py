"""
100 Popular Python Interview Challenges
Each challenge includes:
- Problem description
- Input format
- Expected output format
- Complete solution with examples

Organized by difficulty: Simple (1-35), Intermediate (36-70), Advanced (71-100)
"""

# =============================================================================
# SIMPLE CHALLENGES (1-35) - Basic Python Fundamentals
# =============================================================================

# Challenge 1: Two Sum
"""
Problem: Given an array of integers and a target sum, return indices of two numbers that add up to target.
Input: nums = [2, 7, 11, 15], target = 9
Expected Output: [0, 1] (because nums[0] + nums[1] = 2 + 7 = 9)
"""
def challenge_1():
    nums = [2, 7, 11, 15]
    target = 9
    
    num_dict = {}
    for i, num in enumerate(nums):
        complement = target - num
        if complement in num_dict:
            result = [num_dict[complement], i]
            print(f"Input: nums = {nums}, target = {target}")
            print(f"Output: {result}")
            return result
        num_dict[num] = i
    return []

# Challenge 2: Reverse String
"""
Problem: Write a function that reverses a string.
Input: s = "hello"
Expected Output: "olleh"
"""
def challenge_2():
    s = "hello"
    result = s[::-1]
    print(f"Input: '{s}'")
    print(f"Output: '{result}'")
    return result

# Challenge 3: Palindrome Check
"""
Problem: Given a string, determine if it is a palindrome (reads same forwards and backwards).
Input: s = "racecar"
Expected Output: True
"""
def challenge_3():
    s = "racecar"
    result = s == s[::-1]
    print(f"Input: '{s}'")
    print(f"Output: {result}")
    return result

# Challenge 4: FizzBuzz
"""
Problem: Print numbers 1 to n, replace multiples of 3 with "Fizz", multiples of 5 with "Buzz", multiples of both with "FizzBuzz".
Input: n = 15
Expected Output: [1, 2, "Fizz", 4, "Buzz", "Fizz", 7, 8, "Fizz", "Buzz", 11, "Fizz", 13, 14, "FizzBuzz"]
"""
def challenge_4():
    n = 15
    result = []
    for i in range(1, n + 1):
        if i % 15 == 0:
            result.append("FizzBuzz")
        elif i % 3 == 0:
            result.append("Fizz")
        elif i % 5 == 0:
            result.append("Buzz")
        else:
            result.append(i)
    
    print(f"Input: n = {n}")
    print(f"Output: {result}")
    return result

# Challenge 5: Maximum Number in Array
"""
Problem: Find the maximum number in an array.
Input: nums = [3, 5, 7, 2, 8]
Expected Output: 8
"""
def challenge_5():
    nums = [3, 5, 7, 2, 8]
    result = max(nums)
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 6: Count Vowels
"""
Problem: Count the number of vowels in a string.
Input: s = "programming"
Expected Output: 3 (o, a, i)
"""
def challenge_6():
    s = "programming"
    vowels = "aeiouAEIOU"
    count = sum(1 for char in s if char in vowels)
    print(f"Input: '{s}'")
    print(f"Output: {count}")
    return count

# Challenge 7: Factorial
"""
Problem: Calculate the factorial of a number.
Input: n = 5
Expected Output: 120 (5! = 5*4*3*2*1)
"""
def challenge_7():
    n = 5
    result = 1
    for i in range(1, n + 1):
        result *= i
    print(f"Input: n = {n}")
    print(f"Output: {result}")
    return result

# Challenge 8: Remove Duplicates from List
"""
Problem: Remove duplicate elements from a list while preserving order.
Input: nums = [1, 2, 2, 3, 4, 4, 5]
Expected Output: [1, 2, 3, 4, 5]
"""
def challenge_8():
    nums = [1, 2, 2, 3, 4, 4, 5]
    result = []
    seen = set()
    for num in nums:
        if num not in seen:
            result.append(num)
            seen.add(num)
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 9: Sum of Digits
"""
Problem: Calculate the sum of digits in a number.
Input: n = 12345
Expected Output: 15 (1+2+3+4+5)
"""
def challenge_9():
    n = 12345
    result = sum(int(digit) for digit in str(n))
    print(f"Input: {n}")
    print(f"Output: {result}")
    return result

# Challenge 10: Prime Number Check
"""
Problem: Check if a number is prime.
Input: n = 17
Expected Output: True
"""
def challenge_10():
    n = 17
    if n < 2:
        result = False
    else:
        result = all(n % i != 0 for i in range(2, int(n**0.5) + 1))
    print(f"Input: {n}")
    print(f"Output: {result}")
    return result

# Challenge 11: Fibonacci Sequence
"""
Problem: Generate first n numbers of Fibonacci sequence.
Input: n = 8
Expected Output: [0, 1, 1, 2, 3, 5, 8, 13]
"""
def challenge_11():
    n = 8
    result = []
    a, b = 0, 1
    for _ in range(n):
        result.append(a)
        a, b = b, a + b
    print(f"Input: n = {n}")
    print(f"Output: {result}")
    return result

# Challenge 12: Anagram Check
"""
Problem: Check if two strings are anagrams.
Input: s1 = "listen", s2 = "silent"
Expected Output: True
"""
def challenge_12():
    s1, s2 = "listen", "silent"
    result = sorted(s1.lower()) == sorted(s2.lower())
    print(f"Input: s1 = '{s1}', s2 = '{s2}'")
    print(f"Output: {result}")
    return result

# Challenge 13: Second Largest Number
"""
Problem: Find the second largest number in a list.
Input: nums = [3, 1, 4, 1, 5, 9, 2, 6]
Expected Output: 6
"""
def challenge_13():
    nums = [3, 1, 4, 1, 5, 9, 2, 6]
    unique_nums = list(set(nums))
    unique_nums.sort()
    result = unique_nums[-2]
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 14: Missing Number
"""
Problem: Find the missing number in a sequence from 1 to n.
Input: nums = [1, 2, 4, 5, 6] (missing 3)
Expected Output: 3
"""
def challenge_14():
    nums = [1, 2, 4, 5, 6]
    n = len(nums) + 1
    expected_sum = n * (n + 1) // 2
    actual_sum = sum(nums)
    result = expected_sum - actual_sum
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 15: Count Word Frequency
"""
Problem: Count frequency of each word in a sentence.
Input: sentence = "hello world hello python world"
Expected Output: {'hello': 2, 'world': 2, 'python': 1}
"""
def challenge_15():
    sentence = "hello world hello python world"
    words = sentence.split()
    result = {}
    for word in words:
        result[word] = result.get(word, 0) + 1
    print(f"Input: '{sentence}'")
    print(f"Output: {result}")
    return result

# Challenge 16: List Intersection
"""
Problem: Find common elements between two lists.
Input: list1 = [1, 2, 3, 4], list2 = [3, 4, 5, 6]
Expected Output: [3, 4]
"""
def challenge_16():
    list1 = [1, 2, 3, 4]
    list2 = [3, 4, 5, 6]
    result = list(set(list1) & set(list2))
    print(f"Input: list1 = {list1}, list2 = {list2}")
    print(f"Output: {result}")
    return result

# Challenge 17: Reverse Words in String
"""
Problem: Reverse the order of words in a string.
Input: s = "hello world python"
Expected Output: "python world hello"
"""
def challenge_17():
    s = "hello world python"
    result = ' '.join(s.split()[::-1])
    print(f"Input: '{s}'")
    print(f"Output: '{result}'")
    return result

# Challenge 18: Check Sorted Array
"""
Problem: Check if an array is sorted in ascending order.
Input: nums = [1, 2, 3, 4, 5]
Expected Output: True
"""
def challenge_18():
    nums = [1, 2, 3, 4, 5]
    result = all(nums[i] <= nums[i+1] for i in range(len(nums)-1))
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 19: Capitalize First Letter
"""
Problem: Capitalize the first letter of each word in a string.
Input: s = "hello world python"
Expected Output: "Hello World Python"
"""
def challenge_19():
    s = "hello world python"
    result = s.title()
    print(f"Input: '{s}'")
    print(f"Output: '{result}'")
    return result

# Challenge 20: Sum of Even Numbers
"""
Problem: Calculate sum of all even numbers in a list.
Input: nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
Expected Output: 30 (2+4+6+8+10)
"""
def challenge_20():
    nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    result = sum(num for num in nums if num % 2 == 0)
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 21: List Rotation
"""
Problem: Rotate a list to the right by k positions.
Input: nums = [1, 2, 3, 4, 5, 6, 7], k = 3
Expected Output: [5, 6, 7, 1, 2, 3, 4]
"""
def challenge_21():
    nums = [1, 2, 3, 4, 5, 6, 7]
    k = 3
    k = k % len(nums)
    result = nums[-k:] + nums[:-k]
    print(f"Input: nums = {nums}, k = {k}")
    print(f"Output: {result}")
    return result

# Challenge 22: Perfect Square Check
"""
Problem: Check if a number is a perfect square.
Input: n = 16
Expected Output: True (4*4 = 16)
"""
def challenge_22():
    n = 16
    import math
    sqrt_n = int(math.sqrt(n))
    result = sqrt_n * sqrt_n == n
    print(f"Input: {n}")
    print(f"Output: {result}")
    return result

# Challenge 23: Merge Two Sorted Lists
"""
Problem: Merge two sorted lists into one sorted list.
Input: list1 = [1, 3, 5], list2 = [2, 4, 6]
Expected Output: [1, 2, 3, 4, 5, 6]
"""
def challenge_23():
    list1 = [1, 3, 5]
    list2 = [2, 4, 6]
    result = []
    i, j = 0, 0
    
    while i < len(list1) and j < len(list2):
        if list1[i] <= list2[j]:
            result.append(list1[i])
            i += 1
        else:
            result.append(list2[j])
            j += 1
    
    result.extend(list1[i:])
    result.extend(list2[j:])
    
    print(f"Input: list1 = {list1}, list2 = {list2}")
    print(f"Output: {result}")
    return result

# Challenge 24: GCD (Greatest Common Divisor)
"""
Problem: Find the greatest common divisor of two numbers.
Input: a = 48, b = 18
Expected Output: 6
"""
def challenge_24():
    a, b = 48, 18
    while b:
        a, b = b, a % b
    print(f"Input: a = 48, b = 18")
    print(f"Output: {a}")
    return a

# Challenge 25: Binary to Decimal
"""
Problem: Convert binary number to decimal.
Input: binary = "1010"
Expected Output: 10
"""
def challenge_25():
    binary = "1010"
    result = int(binary, 2)
    print(f"Input: '{binary}'")
    print(f"Output: {result}")
    return result

# Challenge 26: Leap Year Check
"""
Problem: Check if a year is a leap year.
Input: year = 2024
Expected Output: True
"""
def challenge_26():
    year = 2024
    result = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    print(f"Input: {year}")
    print(f"Output: {result}")
    return result

# Challenge 27: Armstrong Number
"""
Problem: Check if a number is an Armstrong number (sum of cubes of digits equals the number).
Input: n = 153
Expected Output: True (1³ + 5³ + 3³ = 1 + 125 + 27 = 153)
"""
def challenge_27():
    n = 153
    digits = [int(d) for d in str(n)]
    power = len(digits)
    sum_of_powers = sum(digit ** power for digit in digits)
    result = sum_of_powers == n
    print(f"Input: {n}")
    print(f"Output: {result}")
    return result

# Challenge 28: Remove Vowels
"""
Problem: Remove all vowels from a string.
Input: s = "programming"
Expected Output: "prgrmmng"
"""
def challenge_28():
    s = "programming"
    vowels = "aeiouAEIOU"
    result = ''.join(char for char in s if char not in vowels)
    print(f"Input: '{s}'")
    print(f"Output: '{result}'")
    return result

# Challenge 29: List Flattening
"""
Problem: Flatten a nested list.
Input: nested_list = [[1, 2], [3, 4], [5, 6]]
Expected Output: [1, 2, 3, 4, 5, 6]
"""
def challenge_29():
    nested_list = [[1, 2], [3, 4], [5, 6]]
    result = [item for sublist in nested_list for item in sublist]
    print(f"Input: {nested_list}")
    print(f"Output: {result}")
    return result

# Challenge 30: Power of Two Check
"""
Problem: Check if a number is a power of 2.
Input: n = 8
Expected Output: True (2³ = 8)
"""
def challenge_30():
    n = 8
    result = n > 0 and (n & (n - 1)) == 0
    print(f"Input: {n}")
    print(f"Output: {result}")
    return result

# Challenge 31: String Compression
"""
Problem: Compress a string by counting consecutive characters.
Input: s = "aabcccccaaa"
Expected Output: "a2b1c5a3"
"""
def challenge_31():
    s = "aabcccccaaa"
    if not s:
        return ""
    
    result = []
    count = 1
    prev_char = s[0]
    
    for i in range(1, len(s)):
        if s[i] == prev_char:
            count += 1
        else:
            result.append(prev_char + str(count))
            prev_char = s[i]
            count = 1
    
    result.append(prev_char + str(count))
    compressed = ''.join(result)
    
    print(f"Input: '{s}'")
    print(f"Output: '{compressed}'")
    return compressed

# Challenge 32: Matrix Transpose
"""
Problem: Transpose a matrix (swap rows and columns).
Input: matrix = [[1, 2, 3], [4, 5, 6]]
Expected Output: [[1, 4], [2, 5], [3, 6]]
"""
def challenge_32():
    matrix = [[1, 2, 3], [4, 5, 6]]
    result = [[matrix[j][i] for j in range(len(matrix))] for i in range(len(matrix[0]))]
    print(f"Input: {matrix}")
    print(f"Output: {result}")
    return result

# Challenge 33: Balanced Parentheses (Simple)
"""
Problem: Check if parentheses are balanced in a string.
Input: s = "((()))"
Expected Output: True
"""
def challenge_33():
    s = "((()))"
    count = 0
    for char in s:
        if char == '(':
            count += 1
        elif char == ')':
            count -= 1
            if count < 0:
                print(f"Input: '{s}'")
                print(f"Output: False")
                return False
    
    result = count == 0
    print(f"Input: '{s}'")
    print(f"Output: {result}")
    return result

# Challenge 34: Find Duplicates
"""
Problem: Find all duplicate elements in a list.
Input: nums = [1, 2, 3, 2, 4, 5, 3]
Expected Output: [2, 3]
"""
def challenge_34():
    nums = [1, 2, 3, 2, 4, 5, 3]
    seen = set()
    duplicates = set()
    
    for num in nums:
        if num in seen:
            duplicates.add(num)
        else:
            seen.add(num)
    
    result = list(duplicates)
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 35: Caesar Cipher
"""
Problem: Implement Caesar cipher with given shift.
Input: text = "hello", shift = 3
Expected Output: "khoor"
"""
def challenge_35():
    text = "hello"
    shift = 3
    result = ""
    
    for char in text:
        if char.isalpha():
            ascii_offset = ord('a') if char.islower() else ord('A')
            shifted = (ord(char) - ascii_offset + shift) % 26
            result += chr(shifted + ascii_offset)
        else:
            result += char
    
    print(f"Input: text = '{text}', shift = {shift}")
    print(f"Output: '{result}'")
    return result

# =============================================================================
# INTERMEDIATE CHALLENGES (36-70) - Data Structures & Algorithms
# =============================================================================

# Challenge 36: Longest Common Subsequence
"""
Problem: Find the length of longest common subsequence between two strings.
Input: text1 = "abcde", text2 = "ace"
Expected Output: 3 (subsequence "ace")
"""
def challenge_36():
    text1, text2 = "abcde", "ace"
    m, n = len(text1), len(text2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if text1[i-1] == text2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])
    
    result = dp[m][n]
    print(f"Input: text1 = '{text1}', text2 = '{text2}'")
    print(f"Output: {result}")
    return result

# Challenge 37: Valid Parentheses (Multiple Types)
"""
Problem: Check if string with multiple types of brackets is valid.
Input: s = "()[]{}"
Expected Output: True
"""
def challenge_37():
    s = "()[]{}"
    stack = []
    mapping = {')': '(', '}': '{', ']': '['}
    
    for char in s:
        if char in mapping:
            if not stack or stack.pop() != mapping[char]:
                print(f"Input: '{s}'")
                print(f"Output: False")
                return False
        else:
            stack.append(char)
    
    result = len(stack) == 0
    print(f"Input: '{s}'")
    print(f"Output: {result}")
    return result

# Challenge 38: Binary Search
"""
Problem: Implement binary search to find target in sorted array.
Input: nums = [1, 3, 5, 7, 9, 11, 13, 15], target = 7
Expected Output: 3 (index of target)
"""
def challenge_38():
    nums = [1, 3, 5, 7, 9, 11, 13, 15]
    target = 7
    left, right = 0, len(nums) - 1
    
    while left <= right:
        mid = (left + right) // 2
        if nums[mid] == target:
            print(f"Input: nums = {nums}, target = {target}")
            print(f"Output: {mid}")
            return mid
        elif nums[mid] < target:
            left = mid + 1
        else:
            right = mid - 1
    
    print(f"Input: nums = {nums}, target = {target}")
    print(f"Output: -1")
    return -1

# Challenge 39: Merge Intervals
"""
Problem: Merge overlapping intervals.
Input: intervals = [[1,3],[2,6],[8,10],[15,18]]
Expected Output: [[1,6],[8,10],[15,18]]
"""
def challenge_39():
    intervals = [[1,3],[2,6],[8,10],[15,18]]
    if not intervals:
        return []
    
    intervals.sort(key=lambda x: x[0])
    merged = [intervals[0]]
    
    for current in intervals[1:]:
        if current[0] <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], current[1])
        else:
            merged.append(current)
    
    print(f"Input: {intervals}")
    print(f"Output: {merged}")
    return merged

# Challenge 40: Longest Substring Without Repeating Characters
"""
Problem: Find length of longest substring without repeating characters.
Input: s = "abcabcbb"
Expected Output: 3 (substring "abc")
"""
def challenge_40():
    s = "abcabcbb"
    char_set = set()
    left = 0
    max_length = 0
    
    for right in range(len(s)):
        while s[right] in char_set:
            char_set.remove(s[left])
            left += 1
        char_set.add(s[right])
        max_length = max(max_length, right - left + 1)
    
    print(f"Input: '{s}'")
    print(f"Output: {max_length}")
    return max_length

# Challenge 41: Three Sum
"""
Problem: Find all unique triplets in array that sum to zero.
Input: nums = [-1, 0, 1, 2, -1, -4]
Expected Output: [[-1, -1, 2], [-1, 0, 1]]
"""
def challenge_41():
    nums = [-1, 0, 1, 2, -1, -4]
    nums.sort()
    result = []
    
    for i in range(len(nums) - 2):
        if i > 0 and nums[i] == nums[i-1]:
            continue
        
        left, right = i + 1, len(nums) - 1
        while left < right:
            total = nums[i] + nums[left] + nums[right]
            if total < 0:
                left += 1
            elif total > 0:
                right -= 1
            else:
                result.append([nums[i], nums[left], nums[right]])
                while left < right and nums[left] == nums[left + 1]:
                    left += 1
                while left < right and nums[right] == nums[right - 1]:
                    right -= 1
                left += 1
                right -= 1
    
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 42: Product of Array Except Self
"""
Problem: Return array where each element is product of all other elements.
Input: nums = [1, 2, 3, 4]
Expected Output: [24, 12, 8, 6]
"""
def challenge_42():
    nums = [1, 2, 3, 4]
    n = len(nums)
    result = [1] * n
    
    # Left pass
    for i in range(1, n):
        result[i] = result[i-1] * nums[i-1]
    
    # Right pass
    right = 1
    for i in range(n-1, -1, -1):
        result[i] *= right
        right *= nums[i]
    
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 43: Container With Most Water
"""
Problem: Find two lines that together with x-axis forms container holding most water.
Input: height = [1,8,6,2,5,4,8,3,7]
Expected Output: 49
"""
def challenge_43():
    height = [1,8,6,2,5,4,8,3,7]
    left, right = 0, len(height) - 1
    max_area = 0
    
    while left < right:
        area = min(height[left], height[right]) * (right - left)
        max_area = max(max_area, area)
        
        if height[left] < height[right]:
            left += 1
        else:
            right -= 1
    
    print(f"Input: {height}")
    print(f"Output: {max_area}")
    return max_area

# Challenge 44: Valid Sudoku
"""
Problem: Determine if a 9x9 Sudoku board is valid.
Input: board = [["5","3",".",".","7",".",".",".","."],["6",".",".","1","9","5",".",".","."],[".","9","8",".",".",".",".","6","."],["8",".",".",".","6",".",".",".","3"],["4",".",".","8",".","3",".",".","1"],["7",".",".",".","2",".",".",".","6"],[".","6",".",".",".",".","2","8","."],[".",".",".","4","1","9",".",".","5"],[".",".",".",".","8",".",".","7","9"]]
Expected Output: True
"""
def challenge_44():
    # Simplified board for display
    board = [
        ["5","3",".",".","7",".",".",".","."],
        ["6",".",".","1","9","5",".",".","."],
        [".","9","8",".",".",".",".","6","."],
        ["8",".",".",".","6",".",".",".","3"],
        ["4",".",".","8",".","3",".",".","1"],
        ["7",".",".",".","2",".",".",".","6"],
        [".","6",".",".",".",".","2","8","."],
        [".",".",".","4","1","9",".",".","5"],
        [".",".",".",".","8",".",".","7","9"]
    ]
    
    def is_valid_sudoku(board):
        rows = [set() for _ in range(9)]
        cols = [set() for _ in range(9)]
        boxes = [set() for _ in range(9)]
        
        for i in range(9):
            for j in range(9):
                if board[i][j] != '.':
                    num = board[i][j]
                    box_index = (i // 3) * 3 + j // 3
                    
                    if num in rows[i] or num in cols[j] or num in boxes[box_index]:
                        return False
                    
                    rows[i].add(num)
                    cols[j].add(num)
                    boxes[box_index].add(num)
        
        return True
    
    result = is_valid_sudoku(board)
    print(f"Input: 9x9 Sudoku board")
    print(f"Output: {result}")
    return result

# Challenge 45: Group Anagrams
"""
Problem: Group strings that are anagrams of each other.
Input: strs = ["eat","tea","tan","ate","nat","bat"]
Expected Output: [["eat","tea","ate"],["tan","nat"],["bat"]]
"""
def challenge_45():
    strs = ["eat","tea","tan","ate","nat","bat"]
    from collections import defaultdict
    
    anagram_groups = defaultdict(list)
    for s in strs:
        key = ''.join(sorted(s))
        anagram_groups[key].append(s)
    
    result = list(anagram_groups.values())
    print(f"Input: {strs}")
    print(f"Output: {result}")
    return result

# Challenge 46: Sliding Window Maximum
"""
Problem: Find maximum in each sliding window of size k.
Input: nums = [1,3,-1,-3,5,3,6,7], k = 3
Expected Output: [3,3,5,5,6,7]
"""
def challenge_46():
    from collections import deque
    
    nums = [1,3,-1,-3,5,3,6,7]
    k = 3
    
    dq = deque()
    result = []
    
    for i in range(len(nums)):
        # Remove indices outside window
        while dq and dq[0] <= i - k:
            dq.popleft()
        
        # Remove smaller elements
        while dq and nums[dq[-1]] <= nums[i]:
            dq.pop()
        
        dq.append(i)
        
        if i >= k - 1:
            result.append(nums[dq[0]])
    
    print(f"Input: nums = {nums}, k = {k}")
    print(f"Output: {result}")
    return result

# Challenge 47: Word Ladder
"""
Problem: Find shortest transformation sequence from beginWord to endWord.
Input: beginWord = "hit", endWord = "cog", wordList = ["hot","dot","dog","lot","log","cog"]
Expected Output: 5 (hit -> hot -> dot -> dog -> cog)
"""
def challenge_47():
    from collections import deque
    
    beginWord = "hit"
    endWord = "cog"
    wordList = ["hot","dot","dog","lot","log","cog"]
    
    if endWord not in wordList:
        return 0
    
    wordSet = set(wordList)
    queue = deque([(beginWord, 1)])
    
    while queue:
        word, length = queue.popleft()
        
        if word == endWord:
            print(f"Input: beginWord = '{beginWord}', endWord = '{endWord}', wordList = {wordList}")
            print(f"Output: {length}")
            return length
        
        for i in range(len(word)):
            for c in 'abcdefghijklmnopqrstuvwxyz':
                next_word = word[:i] + c + word[i+1:]
                if next_word in wordSet:
                    wordSet.remove(next_word)
                    queue.append((next_word, length + 1))
    
    print(f"Input: beginWord = '{beginWord}', endWord = '{endWord}', wordList = {wordList}")
    print(f"Output: 0")
    return 0

# Challenge 48: Top K Frequent Elements
"""
Problem: Find k most frequent elements in array.
Input: nums = [1,1,1,2,2,3], k = 2
Expected Output: [1,2]
"""
def challenge_48():
    from collections import Counter
    import heapq
    
    nums = [1,1,1,2,2,3]
    k = 2
    
    count = Counter(nums)
    result = heapq.nlargest(k, count.keys(), key=count.get)
    
    print(f"Input: nums = {nums}, k = {k}")
    print(f"Output: {result}")
    return result

# Challenge 49: Coin Change
"""
Problem: Find minimum number of coins needed to make amount.
Input: coins = [1, 3, 4], amount = 6
Expected Output: 2 (3 + 3 = 6)
"""
def challenge_49():
    coins = [1, 3, 4]
    amount = 6
    
    dp = [float('inf')] * (amount + 1)
    dp[0] = 0
    
    for i in range(1, amount + 1):
        for coin in coins:
            if i >= coin:
                dp[i] = min(dp[i], dp[i - coin] + 1)
    
    result = dp[amount] if dp[amount] != float('inf') else -1
    print(f"Input: coins = {coins}, amount = {amount}")
    print(f"Output: {result}")
    return result

# Challenge 50: Rotate Image
"""
Problem: Rotate n×n 2D matrix representing an image by 90 degrees clockwise.
Input: matrix = [[1,2,3],[4,5,6],[7,8,9]]
Expected Output: [[7,4,1],[8,5,2],[9,6,3]]
"""
def challenge_50():
    matrix = [[1,2,3],[4,5,6],[7,8,9]]
    n = len(matrix)
    
    # Transpose
    for i in range(n):
        for j in range(i, n):
            matrix[i][j], matrix[j][i] = matrix[j][i], matrix[i][j]
    
    # Reverse each row
    for i in range(n):
        matrix[i].reverse()
    
    print(f"Input: [[1,2,3],[4,5,6],[7,8,9]]")
    print(f"Output: {matrix}")
    return matrix

# Challenge 51: Jump Game
"""
Problem: Determine if you can reach the last index.
Input: nums = [2,3,1,1,4]
Expected Output: True
"""
def challenge_51():
    nums = [2,3,1,1,4]
    max_reach = 0
    
    for i in range(len(nums)):
        if i > max_reach:
            print(f"Input: {nums}")
            print(f"Output: False")
            return False
        max_reach = max(max_reach, i + nums[i])
    
    result = max_reach >= len(nums) - 1
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 52: Unique Paths
"""
Problem: Find number of unique paths in m×n grid from top-left to bottom-right.
Input: m = 3, n = 7
Expected Output: 28
"""
def challenge_52():
    m, n = 3, 7
    
    dp = [[1] * n for _ in range(m)]
    
    for i in range(1, m):
        for j in range(1, n):
            dp[i][j] = dp[i-1][j] + dp[i][j-1]
    
    result = dp[m-1][n-1]
    print(f"Input: m = {m}, n = {n}")
    print(f"Output: {result}")
    return result

# Challenge 53: Word Break
"""
Problem: Determine if string can be segmented into dictionary words.
Input: s = "leetcode", wordDict = ["leet","code"]
Expected Output: True
"""
def challenge_53():
    s = "leetcode"
    wordDict = ["leet","code"]
    
    word_set = set(wordDict)
    dp = [False] * (len(s) + 1)
    dp[0] = True
    
    for i in range(1, len(s) + 1):
        for j in range(i):
            if dp[j] and s[j:i] in word_set:
                dp[i] = True
                break
    
    result = dp[len(s)]
    print(f"Input: s = '{s}', wordDict = {wordDict}")
    print(f"Output: {result}")
    return result

# Challenge 54: House Robber
"""
Problem: Find maximum money that can be robbed without robbing adjacent houses.
Input: nums = [2,7,9,3,1]
Expected Output: 12 (rob houses 0, 2, 4)
"""
def challenge_54():
    nums = [2,7,9,3,1]
    
    if not nums:
        return 0
    if len(nums) == 1:
        return nums[0]
    
    prev2 = nums[0]
    prev1 = max(nums[0], nums[1])
    
    for i in range(2, len(nums)):
        current = max(prev1, prev2 + nums[i])
        prev2 = prev1
        prev1 = current
    
    result = prev1
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 55: Decode Ways
"""
Problem: Count ways to decode a string of digits.
Input: s = "12"
Expected Output: 2 ("AB" (1 2) or "L" (12))
"""
def challenge_55():
    s = "12"
    
    if not s or s[0] == '0':
        return 0
    
    n = len(s)
    dp = [0] * (n + 1)
    dp[0] = dp[1] = 1
    
    for i in range(2, n + 1):
        if s[i-1] != '0':
            dp[i] += dp[i-1]
        
        two_digit = int(s[i-2:i])
        if 10 <= two_digit <= 26:
            dp[i] += dp[i-2]
    
    result = dp[n]
    print(f"Input: '{s}'")
    print(f"Output: {result}")
    return result

# Challenge 56: Maximum Subarray
"""
Problem: Find contiguous subarray with largest sum.
Input: nums = [-2,1,-3,4,-1,2,1,-5,4]
Expected Output: 6 (subarray [4,-1,2,1])
"""
def challenge_56():
    nums = [-2,1,-3,4,-1,2,1,-5,4]
    
    max_so_far = max_ending_here = nums[0]
    
    for i in range(1, len(nums)):
        max_ending_here = max(nums[i], max_ending_here + nums[i])
        max_so_far = max(max_so_far, max_ending_here)
    
    print(f"Input: {nums}")
    print(f"Output: {max_so_far}")
    return max_so_far

# Challenge 57: Spiral Matrix
"""
Problem: Return all elements of matrix in spiral order.
Input: matrix = [[1,2,3],[4,5,6],[7,8,9]]
Expected Output: [1,2,3,6,9,8,7,4,5]
"""
def challenge_57():
    matrix = [[1,2,3],[4,5,6],[7,8,9]]
    
    if not matrix:
        return []
    
    result = []
    top, bottom = 0, len(matrix) - 1
    left, right = 0, len(matrix[0]) - 1
    
    while top <= bottom and left <= right:
        # Right
        for col in range(left, right + 1):
            result.append(matrix[top][col])
        top += 1
        
        # Down
        for row in range(top, bottom + 1):
            result.append(matrix[row][right])
        right -= 1
        
        if top <= bottom:
            # Left
            for col in range(right, left - 1, -1):
                result.append(matrix[bottom][col])
            bottom -= 1
        
        if left <= right:
            # Up
            for row in range(bottom, top - 1, -1):
                result.append(matrix[row][left])
            left += 1
    
    print(f"Input: {matrix}")
    print(f"Output: {result}")
    return result

# Challenge 58: Set Matrix Zeroes
"""
Problem: Set entire row and column to 0 if element is 0.
Input: matrix = [[1,1,1],[1,0,1],[1,1,1]]
Expected Output: [[1,0,1],[0,0,0],[1,0,1]]
"""
def challenge_58():
    matrix = [[1,1,1],[1,0,1],[1,1,1]]
    
    rows, cols = len(matrix), len(matrix[0])
    first_row_zero = any(matrix[0][j] == 0 for j in range(cols))
    first_col_zero = any(matrix[i][0] == 0 for i in range(rows))
    
    # Use first row and column as markers
    for i in range(1, rows):
        for j in range(1, cols):
            if matrix[i][j] == 0:
                matrix[0][j] = 0
                matrix[i][0] = 0
    
    # Set zeros based on markers
    for i in range(1, rows):
        for j in range(1, cols):
            if matrix[0][j] == 0 or matrix[i][0] == 0:
                matrix[i][j] = 0
    
    # Handle first row and column
    if first_row_zero:
        for j in range(cols):
            matrix[0][j] = 0
    
    if first_col_zero:
        for i in range(rows):
            matrix[i][0] = 0
    
    print(f"Input: [[1,1,1],[1,0,1],[1,1,1]]")
    print(f"Output: {matrix}")
    return matrix

# Challenge 59: Search in Rotated Sorted Array
"""
Problem: Search target in rotated sorted array.
Input: nums = [4,5,6,7,0,1,2], target = 0
Expected Output: 4
"""
def challenge_59():
    nums = [4,5,6,7,0,1,2]
    target = 0
    
    left, right = 0, len(nums) - 1
    
    while left <= right:
        mid = (left + right) // 2
        
        if nums[mid] == target:
            print(f"Input: nums = {nums}, target = {target}")
            print(f"Output: {mid}")
            return mid
        
        # Left half is sorted
        if nums[left] <= nums[mid]:
            if nums[left] <= target < nums[mid]:
                right = mid - 1
            else:
                left = mid + 1
        # Right half is sorted
        else:
            if nums[mid] < target <= nums[right]:
                left = mid + 1
            else:
                right = mid - 1
    
    print(f"Input: nums = {nums}, target = {target}")
    print(f"Output: -1")
    return -1

# Challenge 60: Generate Parentheses
"""
Problem: Generate all combinations of well-formed parentheses.
Input: n = 3
Expected Output: ["((()))","(()())","(())()","()(())","()()()"]
"""
def challenge_60():
    n = 3
    result = []
    
    def backtrack(s, left, right):
        if len(s) == 2 * n:
            result.append(s)
            return
        
        if left < n:
            backtrack(s + '(', left + 1, right)
        
        if right < left:
            backtrack(s + ')', left, right + 1)
    
    backtrack('', 0, 0)
    
    print(f"Input: n = {n}")
    print(f"Output: {result}")
    return result

# Challenge 61: Letter Combinations of Phone Number
"""
Problem: Return all possible letter combinations for a phone number.
Input: digits = "23"
Expected Output: ["ad","ae","af","bd","be","bf","cd","ce","cf"]
"""
def challenge_61():
    digits = "23"
    
    if not digits:
        return []
    
    phone = {
        '2': 'abc', '3': 'def', '4': 'ghi', '5': 'jkl',
        '6': 'mno', '7': 'pqrs', '8': 'tuv', '9': 'wxyz'
    }
    
    result = []
    
    def backtrack(index, path):
        if index == len(digits):
            result.append(path)
            return
        
        for letter in phone[digits[index]]:
            backtrack(index + 1, path + letter)
    
    backtrack(0, '')
    
    print(f"Input: '{digits}'")
    print(f"Output: {result}")
    return result

# Challenge 62: Permutations
"""
Problem: Generate all permutations of a list.
Input: nums = [1,2,3]
Expected Output: [[1,2,3],[1,3,2],[2,1,3],[2,3,1],[3,1,2],[3,2,1]]
"""
def challenge_62():
    nums = [1,2,3]
    result = []
    
    def backtrack(path):
        if len(path) == len(nums):
            result.append(path[:])
            return
        
        for num in nums:
            if num not in path:
                path.append(num)
                backtrack(path)
                path.pop()
    
    backtrack([])
    
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 63: Subsets
"""
Problem: Generate all possible subsets.
Input: nums = [1,2,3]
Expected Output: [[],[1],[2],[1,2],[3],[1,3],[2,3],[1,2,3]]
"""
def challenge_63():
    nums = [1,2,3]
    result = []
    
    def backtrack(start, path):
        result.append(path[:])
        
        for i in range(start, len(nums)):
            path.append(nums[i])
            backtrack(i + 1, path)
            path.pop()
    
    backtrack(0, [])
    
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 64: Combination Sum
"""
Problem: Find all unique combinations that sum to target.
Input: candidates = [2,3,6,7], target = 7
Expected Output: [[2,2,3],[7]]
"""
def challenge_64():
    candidates = [2,3,6,7]
    target = 7
    result = []
    
    def backtrack(start, path, remaining):
        if remaining == 0:
            result.append(path[:])
            return
        
        if remaining < 0:
            return
        
        for i in range(start, len(candidates)):
            path.append(candidates[i])
            backtrack(i, path, remaining - candidates[i])
            path.pop()
    
    backtrack(0, [], target)
    
    print(f"Input: candidates = {candidates}, target = {target}")
    print(f"Output: {result}")
    return result

# Challenge 65: Sort Colors
"""
Problem: Sort array with only 0s, 1s, and 2s in-place.
Input: nums = [2,0,2,1,1,0]
Expected Output: [0,0,1,1,2,2]
"""
def challenge_65():
    nums = [2,0,2,1,1,0]
    
    left, current, right = 0, 0, len(nums) - 1
    
    while current <= right:
        if nums[current] == 0:
            nums[left], nums[current] = nums[current], nums[left]
            left += 1
            current += 1
        elif nums[current] == 1:
            current += 1
        else:
            nums[current], nums[right] = nums[right], nums[current]
            right -= 1
    
    print(f"Input: [2,0,2,1,1,0]")
    print(f"Output: {nums}")
    return nums

# Challenge 66: Find Peak Element
"""
Problem: Find a peak element (greater than neighbors).
Input: nums = [1,2,3,1]
Expected Output: 2 (index of peak element 3)
"""
def challenge_66():
    nums = [1,2,3,1]
    
    left, right = 0, len(nums) - 1
    
    while left < right:
        mid = (left + right) // 2
        
        if nums[mid] > nums[mid + 1]:
            right = mid
        else:
            left = mid + 1
    
    print(f"Input: {nums}")
    print(f"Output: {left}")
    return left

# Challenge 67: Search 2D Matrix
"""
Problem: Search target in sorted 2D matrix.
Input: matrix = [[1,4,7,11],[2,5,8,12],[3,6,9,16],[10,13,14,17]], target = 5
Expected Output: True
"""
def challenge_67():
    matrix = [[1,4,7,11],[2,5,8,12],[3,6,9,16],[10,13,14,17]]
    target = 5
    
    if not matrix or not matrix[0]:
        return False
    
    row, col = 0, len(matrix[0]) - 1
    
    while row < len(matrix) and col >= 0:
        if matrix[row][col] == target:
            print(f"Input: matrix (4x4), target = {target}")
            print(f"Output: True")
            return True
        elif matrix[row][col] > target:
            col -= 1
        else:
            row += 1
    
    print(f"Input: matrix (4x4), target = {target}")
    print(f"Output: False")
    return False

# Challenge 68: Kth Largest Element
"""
Problem: Find kth largest element in unsorted array.
Input: nums = [3,2,1,5,6,4], k = 2
Expected Output: 5
"""
def challenge_68():
    import heapq
    
    nums = [3,2,1,5,6,4]
    k = 2
    
    result = heapq.nlargest(k, nums)[k-1]
    
    print(f"Input: nums = {nums}, k = {k}")
    print(f"Output: {result}")
    return result

# Challenge 69: Meeting Rooms II
"""
Problem: Find minimum number of meeting rooms required.
Input: intervals = [[0,30],[5,10],[15,20]]
Expected Output: 2
"""
def challenge_69():
    import heapq
    
    intervals = [[0,30],[5,10],[15,20]]
    
    if not intervals:
        return 0
    
    intervals.sort(key=lambda x: x[0])
    heap = []
    
    for interval in intervals:
        if heap and heap[0] <= interval[0]:
            heapq.heappop(heap)
        heapq.heappush(heap, interval[1])
    
    result = len(heap)
    print(f"Input: {intervals}")
    print(f"Output: {result}")
    return result

# Challenge 70: Longest Palindromic Substring
"""
Problem: Find the longest palindromic substring.
Input: s = "babad"
Expected Output: "bab" (or "aba")
"""
def challenge_70():
    s = "babad"
    
    def expand_around_center(left, right):
        while left >= 0 and right < len(s) and s[left] == s[right]:
            left -= 1
            right += 1
        return s[left + 1:right]
    
    longest = ""
    
    for i in range(len(s)):
        # Odd length palindromes
        palindrome1 = expand_around_center(i, i)
        # Even length palindromes
        palindrome2 = expand_around_center(i, i + 1)
        
        for palindrome in [palindrome1, palindrome2]:
            if len(palindrome) > len(longest):
                longest = palindrome
    
    print(f"Input: '{s}'")
    print(f"Output: '{longest}'")
    return longest

# =============================================================================
# ADVANCED CHALLENGES (71-100) - Complex Algorithms & Data Structures
# =============================================================================

# Challenge 71: Serialize and Deserialize Binary Tree
"""
Problem: Design algorithm to serialize and deserialize binary tree.
Input: root = [1,2,3,null,null,4,5]
Expected Output: Serialized string that can be deserialized back to original tree
"""
def challenge_71():
    class TreeNode:
        def __init__(self, val=0, left=None, right=None):
            self.val = val
            self.left = left
            self.right = right
    
    def serialize(root):
        def preorder(node):
            if not node:
                return "null,"
            return str(node.val) + "," + preorder(node.left) + preorder(node.right)
        return preorder(root)
    
    def deserialize(data):
        def build_tree(nodes):
            val = next(nodes)
            if val == "null":
                return None
            node = TreeNode(int(val))
            node.left = build_tree(nodes)
            node.right = build_tree(nodes)
            return node
        return build_tree(iter(data.split(",")))
    
    # Test
    root = TreeNode(1)
    root.left = TreeNode(2)
    root.right = TreeNode(3)
    root.right.left = TreeNode(4)
    root.right.right = TreeNode(5)
    
    serialized = serialize(root)
    deserialized = deserialize(serialized)
    
    print(f"Input: Binary tree [1,2,3,null,null,4,5]")
    print(f"Output: Serialized and deserialized successfully")
    return serialized

# Challenge 72: Median of Two Sorted Arrays
"""
Problem: Find median of two sorted arrays.
Input: nums1 = [1,3], nums2 = [2]
Expected Output: 2.0
"""
def challenge_72():
    nums1 = [1,3]
    nums2 = [2]
    
    # Ensure nums1 is smaller
    if len(nums1) > len(nums2):
        nums1, nums2 = nums2, nums1
    
    m, n = len(nums1), len(nums2)
    left, right = 0, m
    
    while left <= right:
        partition_x = (left + right) // 2
        partition_y = (m + n + 1) // 2 - partition_x
        
        max_left_x = float('-inf') if partition_x == 0 else nums1[partition_x - 1]
        min_right_x = float('inf') if partition_x == m else nums1[partition_x]
        
        max_left_y = float('-inf') if partition_y == 0 else nums2[partition_y - 1]
        min_right_y = float('inf') if partition_y == n else nums2[partition_y]
        
        if max_left_x <= min_right_y and max_left_y <= min_right_x:
            if (m + n) % 2 == 0:
                result = (max(max_left_x, max_left_y) + min(min_right_x, min_right_y)) / 2
            else:
                result = max(max_left_x, max_left_y)
            
            print(f"Input: nums1 = {nums1}, nums2 = {nums2}")
            print(f"Output: {result}")
            return result
        elif max_left_x > min_right_y:
            right = partition_x - 1
        else:
            left = partition_x + 1

# Challenge 73: Regular Expression Matching
"""
Problem: Implement regular expression matching with '.' and '*'.
Input: s = "aa", p = "a*"
Expected Output: True
"""
def challenge_73():
    s = "aa"
    p = "a*"
    
    def is_match(s, p):
        if not p:
            return not s
        
        first_match = bool(s) and (p[0] == s[0] or p[0] == '.')
        
        if len(p) >= 2 and p[1] == '*':
            return (is_match(s, p[2:]) or 
                   (first_match and is_match(s[1:], p)))
        else:
            return first_match and is_match(s[1:], p[1:])
    
    result = is_match(s, p)
    print(f"Input: s = '{s}', p = '{p}'")
    print(f"Output: {result}")
    return result

# Challenge 74: Trapping Rain Water
"""
Problem: Calculate how much rain water can be trapped.
Input: height = [0,1,0,2,1,0,1,3,2,1,2,1]
Expected Output: 6
"""
def challenge_74():
    height = [0,1,0,2,1,0,1,3,2,1,2,1]
    
    left, right = 0, len(height) - 1
    left_max = right_max = 0
    water = 0
    
    while left < right:
        if height[left] < height[right]:
            if height[left] >= left_max:
                left_max = height[left]
            else:
                water += left_max - height[left]
            left += 1
        else:
            if height[right] >= right_max:
                right_max = height[right]
            else:
                water += right_max - height[right]
            right -= 1
    
    print(f"Input: {height}")
    print(f"Output: {water}")
    return water

# Challenge 75: Minimum Window Substring
"""
Problem: Find minimum window in s that contains all characters of t.
Input: s = "ADOBECODEBANC", t = "ABC"
Expected Output: "BANC"
"""
def challenge_75():
    from collections import Counter
    
    s = "ADOBECODEBANC"
    t = "ABC"
    
    if not s or not t:
        return ""
    
    dict_t = Counter(t)
    required = len(dict_t)
    
    left = right = 0
    formed = 0
    window_counts = {}
    
    ans = float("inf"), None, None
    
    while right < len(s):
        char = s[right]
        window_counts[char] = window_counts.get(char, 0) + 1
        
        if char in dict_t and window_counts[char] == dict_t[char]:
            formed += 1
        
        while left <= right and formed == required:
            char = s[left]
            
            if right - left + 1 < ans[0]:
                ans = (right - left + 1, left, right)
            
            window_counts[char] -= 1
            if char in dict_t and window_counts[char] < dict_t[char]:
                formed -= 1
            
            left += 1
        
        right += 1
    
    result = "" if ans[0] == float("inf") else s[ans[1] : ans[2] + 1]
    print(f"Input: s = '{s}', t = '{t}'")
    print(f"Output: '{result}'")
    return result

# Challenge 76: Edit Distance
"""
Problem: Find minimum edit distance between two strings.
Input: word1 = "horse", word2 = "ros"
Expected Output: 3
"""
def challenge_76():
    word1 = "horse"
    word2 = "ros"
    
    m, n = len(word1), len(word2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if word1[i-1] == word2[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(dp[i-1][j], dp[i][j-1], dp[i-1][j-1])
    
    result = dp[m][n]
    print(f"Input: word1 = '{word1}', word2 = '{word2}'")
    print(f"Output: {result}")
    return result

# Challenge 77: Largest Rectangle in Histogram
"""
Problem: Find area of largest rectangle in histogram.
Input: heights = [2,1,5,6,2,3]
Expected Output: 10
"""
def challenge_77():
    heights = [2,1,5,6,2,3]
    
    stack = []
    max_area = 0
    
    for i, h in enumerate(heights):
        while stack and heights[stack[-1]] > h:
            height = heights[stack.pop()]
            width = i if not stack else i - stack[-1] - 1
            max_area = max(max_area, height * width)
        stack.append(i)
    
    while stack:
        height = heights[stack.pop()]
        width = len(heights) if not stack else len(heights) - stack[-1] - 1
        max_area = max(max_area, height * width)
    
    print(f"Input: {heights}")
    print(f"Output: {max_area}")
    return max_area

# Challenge 78: N-Queens
"""
Problem: Solve N-Queens problem.
Input: n = 4
Expected Output: [[".Q..","...Q","Q...","..Q."],["..Q.","Q...","...Q",".Q.."]]
"""
def challenge_78():
    n = 4
    
    def solve_n_queens(n):
        def is_safe(board, row, col):
            for i in range(row):
                if board[i][col] == 'Q':
                    return False
            
            for i, j in zip(range(row-1, -1, -1), range(col-1, -1, -1)):
                if board[i][j] == 'Q':
                    return False
            
            for i, j in zip(range(row-1, -1, -1), range(col+1, n)):
                if board[i][j] == 'Q':
                    return False
            
            return True
        
        def solve(board, row):
            if row == n:
                result.append([''.join(row) for row in board])
                return
            
            for col in range(n):
                if is_safe(board, row, col):
                    board[row][col] = 'Q'
                    solve(board, row + 1)
                    board[row][col] = '.'
        
        result = []
        board = [['.' for _ in range(n)] for _ in range(n)]
        solve(board, 0)
        return result
    
    solutions = solve_n_queens(n)
    print(f"Input: n = {n}")
    print(f"Output: {len(solutions)} solutions found")
    return solutions

# Challenge 79: Word Search II
"""
Problem: Find all words in 2D board.
Input: board = [["o","a","a","n"],["e","t","a","e"],["i","h","k","r"],["i","f","l","v"]], words = ["oath","pea","eat","rain"]
Expected Output: ["eat","oath"]
"""
def challenge_79():
    board = [["o","a","a","n"],["e","t","a","e"],["i","h","k","r"],["i","f","l","v"]]
    words = ["oath","pea","eat","rain"]
    
    class TrieNode:
        def __init__(self):
            self.children = {}
            self.word = None
    
    def build_trie(words):
        root = TrieNode()
        for word in words:
            node = root
            for char in word:
                if char not in node.children:
                    node.children[char] = TrieNode()
                node = node.children[char]
            node.word = word
        return root
    
    def dfs(board, i, j, parent, result):
        char = board[i][j]
        node = parent.children[char]
        
        if node.word:
            result.append(node.word)
            node.word = None
        
        board[i][j] = '#'
        
        for di, dj in [(0, 1), (1, 0), (0, -1), (-1, 0)]:
            ni, nj = i + di, j + dj
            if (0 <= ni < len(board) and 0 <= nj < len(board[0]) and 
                board[ni][nj] in node.children):
                dfs(board, ni, nj, node, result)
        
        board[i][j] = char
    
    root = build_trie(words)
    result = []
    
    for i in range(len(board)):
        for j in range(len(board[0])):
            if board[i][j] in root.children:
                dfs(board, i, j, root, result)
    
    print(f"Input: board (4x4), words = {words}")
    print(f"Output: {result}")
    return result

# Challenge 80: LRU Cache
"""
Problem: Design and implement LRU (Least Recently Used) cache.
Input: capacity = 2, operations = [put(1,1), put(2,2), get(1), put(3,3), get(2), put(4,4), get(1), get(3), get(4)]
Expected Output: [null, null, 1, null, -1, null, -1, 3, 4]
"""
def challenge_80():
    class LRUCache:
        def __init__(self, capacity):
            self.capacity = capacity
            self.cache = {}
            self.order = []
        
        def get(self, key):
            if key in self.cache:
                self.order.remove(key)
                self.order.append(key)
                return self.cache[key]
            return -1
        
        def put(self, key, value):
            if key in self.cache:
                self.order.remove(key)
            elif len(self.cache) >= self.capacity:
                oldest = self.order.pop(0)
                del self.cache[oldest]
            
            self.cache[key] = value
            self.order.append(key)
    
    lru = LRUCache(2)
    operations = [
        ("put", 1, 1), ("put", 2, 2), ("get", 1), ("put", 3, 3),
        ("get", 2), ("put", 4, 4), ("get", 1), ("get", 3), ("get", 4)
    ]
    
    results = []
    for op in operations:
        if op[0] == "put":
            lru.put(op[1], op[2])
            results.append(None)
        else:
            results.append(lru.get(op[1]))
    
    print(f"Input: capacity = 2, operations = {len(operations)} operations")
    print(f"Output: {results}")
    return results

# Challenge 81: Alien Dictionary
"""
Problem: Find order of characters in alien language.
Input: words = ["wrt","wrf","er","ett","rftt"]
Expected Output: "wertf"
"""
def challenge_81():
    from collections import defaultdict, deque
    
    words = ["wrt","wrf","er","ett","rftt"]
    
    graph = defaultdict(set)
    in_degree = defaultdict(int)
    chars = set()
    
    for word in words:
        for char in word:
            chars.add(char)
    
    for char in chars:
        in_degree[char] = 0
    
    for i in range(len(words) - 1):
        w1, w2 = words[i], words[i + 1]
        min_len = min(len(w1), len(w2))
        
        if len(w1) > len(w2) and w1[:min_len] == w2[:min_len]:
            return ""
        
        for j in range(min_len):
            if w1[j] != w2[j]:
                if w2[j] not in graph[w1[j]]:
                    graph[w1[j]].add(w2[j])
                    in_degree[w2[j]] += 1
                break
    
    queue = deque([char for char in chars if in_degree[char] == 0])
    result = []
    
    while queue:
        char = queue.popleft()
        result.append(char)
        
        for neighbor in graph[char]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    if len(result) != len(chars):
        return ""
    
    alien_order = ''.join(result)
    print(f"Input: {words}")
    print(f"Output: '{alien_order}'")
    return alien_order

# Challenge 82: Course Schedule II
"""
Problem: Find order to finish all courses.
Input: numCourses = 4, prerequisites = [[1,0],[2,0],[3,1],[3,2]]
Expected Output: [0,2,1,3]
"""
def challenge_82():
    from collections import defaultdict, deque
    
    numCourses = 4
    prerequisites = [[1,0],[2,0],[3,1],[3,2]]
    
    graph = defaultdict(list)
    in_degree = [0] * numCourses
    
    for course, prereq in prerequisites:
        graph[prereq].append(course)
        in_degree[course] += 1
    
    queue = deque([i for i in range(numCourses) if in_degree[i] == 0])
    result = []
    
    while queue:
        course = queue.popleft()
        result.append(course)
        
        for next_course in graph[course]:
            in_degree[next_course] -= 1
            if in_degree[next_course] == 0:
                queue.append(next_course)
    
    if len(result) != numCourses:
        return []
    
    print(f"Input: numCourses = {numCourses}, prerequisites = {prerequisites}")
    print(f"Output: {result}")
    return result

# Challenge 83: Design Twitter
"""
Problem: Design a simplified version of Twitter.
Input: Various operations like postTweet, getNewsFeed, follow, unfollow
Expected Output: News feed with 10 most recent tweets
"""
def challenge_83():
    import heapq
    from collections import defaultdict
    
    class Twitter:
        def __init__(self):
            self.tweets = defaultdict(list)
            self.following = defaultdict(set)
            self.time = 0
        
        def postTweet(self, userId, tweetId):
            self.tweets[userId].append((self.time, tweetId))
            self.time += 1
        
        def getNewsFeed(self, userId):
            heap = []
            
            for time, tweetId in self.tweets[userId]:
                heapq.heappush(heap, (-time, tweetId))
            
            for followeeId in self.following[userId]:
                for time, tweetId in self.tweets[followeeId]:
                    heapq.heappush(heap, (-time, tweetId))
            
            feed = []
            for _ in range(min(10, len(heap))):
                if heap:
                    _, tweetId = heapq.heappop(heap)
                    feed.append(tweetId)
            
            return feed
        
        def follow(self, followerId, followeeId):
            if followerId != followeeId:
                self.following[followerId].add(followeeId)
        
        def unfollow(self, followerId, followeeId):
            self.following[followerId].discard(followeeId)
    
    twitter = Twitter()
    operations = [
        ("postTweet", 1, 5),
        ("getNewsFeed", 1),
        ("follow", 1, 2),
        ("postTweet", 2, 6),
        ("getNewsFeed", 1),
        ("unfollow", 1, 2),
        ("getNewsFeed", 1)
    ]
    
    results = []
    for op in operations:
        if op[0] == "postTweet":
            twitter.postTweet(op[1], op[2])
            results.append(None)
        elif op[0] == "getNewsFeed":
            feed = twitter.getNewsFeed(op[1])
            results.append(feed)
        elif op[0] == "follow":
            twitter.follow(op[1], op[2])
            results.append(None)
        elif op[0] == "unfollow":
            twitter.unfollow(op[1], op[2])
            results.append(None)
    
    print(f"Input: {len(operations)} Twitter operations")
    print(f"Output: {results}")
    return results

# Challenge 84: Maximum Profit in Job Scheduling
"""
Problem: Find maximum profit from non-overlapping jobs.
Input: startTime = [1,2,3,3], endTime = [3,4,5,6], profit = [50,10,40,70]
Expected Output: 120
"""
def challenge_84():
    startTime = [1,2,3,3]
    endTime = [3,4,5,6]
    profit = [50,10,40,70]
    
    jobs = list(zip(startTime, endTime, profit))
    jobs.sort(key=lambda x: x[1])  # Sort by end time
    
    n = len(jobs)
    dp = [0] * n
    dp[0] = jobs[0][2]
    
    def binary_search(jobs, i):
        left, right = 0, i - 1
        result = -1
        
        while left <= right:
            mid = (left + right) // 2
            if jobs[mid][1] <= jobs[i][0]:
                result = mid
                left = mid + 1
            else:
                right = mid - 1
        
        return result
    
    for i in range(1, n):
        include_profit = jobs[i][2]
        latest_non_conflict = binary_search(jobs, i)
        
        if latest_non_conflict != -1:
            include_profit += dp[latest_non_conflict]
        
        dp[i] = max(dp[i-1], include_profit)
    
    result = dp[n-1]
    print(f"Input: startTime = {startTime}, endTime = {endTime}, profit = {profit}")
    print(f"Output: {result}")
    return result

# Challenge 85: Sliding Window Median
"""
Problem: Find median in each sliding window.
Input: nums = [1,3,-1,-3,5,3,6,7], k = 3
Expected Output: [1,-1,-1,3,5,6]
"""
def challenge_85():
    import heapq
    
    nums = [1,3,-1,-3,5,3,6,7]
    k = 3
    
    def find_medians(nums, k):
        result = []
        
        for i in range(len(nums) - k + 1):
            window = sorted(nums[i:i+k])
            if k % 2 == 1:
                median = window[k // 2]
            else:
                median = (window[k // 2 - 1] + window[k // 2]) // 2
            result.append(median)
        
        return result
    
    result = find_medians(nums, k)
    print(f"Input: nums = {nums}, k = {k}")
    print(f"Output: {result}")
    return result

# Challenge 86: Expression Add Operators
"""
Problem: Add operators (+, -, *) to make expression equal target.
Input: num = "123", target = 6
Expected Output: ["1+2+3", "1*2*3"]
"""
def challenge_86():
    num = "123"
    target = 6
    
    def add_operators(num, target):
        result = []
        
        def backtrack(index, path, value, prev):
            if index == len(num):
                if value == target:
                    result.append(path)
                return
            
            for i in range(index, len(num)):
                curr_str = num[index:i+1]
                
                if len(curr_str) > 1 and curr_str[0] == '0':
                    break
                
                curr_num = int(curr_str)
                
                if index == 0:
                    backtrack(i + 1, curr_str, curr_num, curr_num)
                else:
                    backtrack(i + 1, path + '+' + curr_str, value + curr_num, curr_num)
                    backtrack(i + 1, path + '-' + curr_str, value - curr_num, -curr_num)
                    backtrack(i + 1, path + '*' + curr_str, value - prev + prev * curr_num, prev * curr_num)
        
        backtrack(0, "", 0, 0)
        return result
    
    expressions = add_operators(num, target)
    print(f"Input: num = '{num}', target = {target}")
    print(f"Output: {expressions}")
    return expressions

# Challenge 87: Range Sum Query - Mutable
"""
Problem: Design data structure for range sum queries with updates.
Input: nums = [1, 3, 5], operations = [sumRange(0,2), update(1,2), sumRange(0,2)]
Expected Output: [9, null, 8]
"""
def challenge_87():
    class NumArray:
        def __init__(self, nums):
            self.nums = nums[:]
        
        def update(self, index, val):
            self.nums[index] = val
        
        def sumRange(self, left, right):
            return sum(self.nums[left:right+1])
    
    nums = [1, 3, 5]
    num_array = NumArray(nums)
    
    operations = [
        ("sumRange", 0, 2),
        ("update", 1, 2),
        ("sumRange", 0, 2)
    ]
    
    results = []
    for op in operations:
        if op[0] == "sumRange":
            result = num_array.sumRange(op[1], op[2])
            results.append(result)
        else:
            num_array.update(op[1], op[2])
            results.append(None)
    
    print(f"Input: nums = {nums}, {len(operations)} operations")
    print(f"Output: {results}")
    return results

# Challenge 88: Basic Calculator II
"""
Problem: Implement a basic calculator to evaluate expression string.
Input: s = "3+2*2"
Expected Output: 7
"""
def challenge_88():
    s = "3+2*2"
    
    def calculate(s):
        stack = []
        num = 0
        sign = '+'
        
        for i, char in enumerate(s):
            if char.isdigit():
                num = num * 10 + int(char)
            
            if char in '+-*/' or i == len(s) - 1:
                if sign == '+':
                    stack.append(num)
                elif sign == '-':
                    stack.append(-num)
                elif sign == '*':
                    stack.append(stack.pop() * num)
                elif sign == '/':
                    stack.append(int(stack.pop() / num))
                
                sign = char
                num = 0
        
        return sum(stack)
    
    result = calculate(s)
    print(f"Input: '{s}'")
    print(f"Output: {result}")
    return result

# Challenge 89: Reconstruct Itinerary
"""
Problem: Reconstruct itinerary from airline tickets.
Input: tickets = [["MUC","LHR"],["JFK","MUC"],["SFO","SJC"],["LHR","SFO"]]
Expected Output: ["JFK","MUC","LHR","SFO","SJC"]
"""
def challenge_89():
    from collections import defaultdict
    
    tickets = [["MUC","LHR"],["JFK","MUC"],["SFO","SJC"],["LHR","SFO"]]
    
    graph = defaultdict(list)
    for start, end in tickets:
        graph[start].append(end)
    
    for start in graph:
        graph[start].sort()
    
    result = []
    
    def dfs(airport):
        while graph[airport]:
            next_airport = graph[airport].pop(0)
            dfs(next_airport)
        result.append(airport)
    
    dfs("JFK")
    
    itinerary = result[::-1]
    print(f"Input: {tickets}")
    print(f"Output: {itinerary}")
    return itinerary

# Challenge 90: Count of Smaller Numbers After Self
"""
Problem: Count how many numbers after each element are smaller than it.
Input: nums = [5,2,6,1]
Expected Output: [2,1,1,0]
"""
def challenge_90():
    nums = [5,2,6,1]
    
    def merge_sort(arr):
        if len(arr) <= 1:
            return arr
        
        mid = len(arr) // 2
        left = merge_sort(arr[:mid])
        right = merge_sort(arr[mid:])
        
        merged = []
        i = j = 0
        
        while i < len(left) and j < len(right):
            if left[i][0] <= right[j][0]:
                merged.append(left[i])
                counts[left[i][1]] += j
                i += 1
            else:
                merged.append(right[j])
                j += 1
        
        while i < len(left):
            merged.append(left[i])
            counts[left[i][1]] += j
            i += 1
        
        while j < len(right):
            merged.append(right[j])
            j += 1
        
        return merged
    
    indexed_nums = [(nums[i], i) for i in range(len(nums))]
    counts = [0] * len(nums)
    
    merge_sort(indexed_nums)
    
    print(f"Input: {nums}")
    print(f"Output: {counts}")
    return counts

# Challenge 91: Russian Doll Envelopes
"""
Problem: Find maximum number of envelopes that can be Russian dolled.
Input: envelopes = [[5,4],[6,4],[6,7],[2,3]]
Expected Output: 3
"""
def challenge_91():
    import bisect
    
    envelopes = [[5,4],[6,4],[6,7],[2,3]]
    
    envelopes.sort(key=lambda x: (x[0], -x[1]))
    
    def lis_length(arr):
        tails = []
        for num in arr:
            pos = bisect.bisect_left(tails, num)
            if pos == len(tails):
                tails.append(num)
            else:
                tails[pos] = num
        return len(tails)
    
    heights = [envelope[1] for envelope in envelopes]
    result = lis_length(heights)
    
    print(f"Input: {envelopes}")
    print(f"Output: {result}")
    return result

# Challenge 92: Design Search Autocomplete System
"""
Problem: Design search autocomplete system.
Input: sentences = ["i love you", "island","ironman"], times = [5,3,2], input characters
Expected Output: Top 3 hot sentences for each character input
"""
def challenge_92():
    class AutocompleteSystem:
        def __init__(self, sentences, times):
            self.history = {}
            for i, sentence in enumerate(sentences):
                self.history[sentence] = times[i]
            self.current_input = ""
        
        def input(self, c):
            if c == '#':
                if self.current_input:
                    self.history[self.current_input] = self.history.get(self.current_input, 0) + 1
                self.current_input = ""
                return []
            
            self.current_input += c
            
            candidates = []
            for sentence, count in self.history.items():
                if sentence.startswith(self.current_input):
                    candidates.append((count, sentence))
            
            candidates.sort(key=lambda x: (-x[0], x[1]))
            
            return [sentence for _, sentence in candidates[:3]]
    
    system = AutocompleteSystem(["i love you", "island", "ironman"], [5, 3, 2])
    
    test_inputs = ['i', ' ', 'a', '#', 'i', ' ', 'a', '#', 'i', ' ', 'a', '#']
    results = []
    
    for char in test_inputs[:4]:  # Test first few inputs
        result = system.input(char)
        results.append(f"input('{char}'): {result}")
    
    print(f"Input: sentences and times, test inputs")
    print(f"Output: {results}")
    return results

# Challenge 93: Palindrome Pairs
"""
Problem: Find all pairs of strings that form palindromes when concatenated.
Input: words = ["lls","s","sssll"]
Expected Output: [[0,1],[1,0],[0,2],[2,0]]
"""
def challenge_93():
    words = ["lls","s","sssll"]
    
    def is_palindrome(s):
        return s == s[::-1]
    
    result = []
    
    for i in range(len(words)):
        for j in range(len(words)):
            if i != j:
                combined = words[i] + words[j]
                if is_palindrome(combined):
                    result.append([i, j])
    
    print(f"Input: {words}")
    print(f"Output: {result}")
    return result

# Challenge 94: Burst Balloons
"""
Problem: Find maximum coins you can collect by bursting balloons.
Input: nums = [3,1,5,8]
Expected Output: 167
"""
def challenge_94():
    nums = [3,1,5,8]
    
    balloons = [1] + nums + [1]
    n = len(balloons)
    
    dp = [[0] * n for _ in range(n)]
    
    for length in range(2, n):
        for i in range(n - length):
            j = i + length
            for k in range(i + 1, j):
                coins = balloons[i] * balloons[k] * balloons[j]
                dp[i][j] = max(dp[i][j], dp[i][k] + dp[k][j] + coins)
    
    result = dp[0][n-1]
    print(f"Input: {nums}")
    print(f"Output: {result}")
    return result

# Challenge 95: Remove Invalid Parentheses
"""
Problem: Remove minimum invalid parentheses to make string valid.
Input: s = "()())"
Expected Output: ["()()", "(())"]
"""
def challenge_95():
    from collections import deque
    
    s = "()())"
    
    def is_valid(s):
        count = 0
        for char in s:
            if char == '(':
                count += 1
            elif char == ')':
                count -= 1
                if count < 0:
                    return False
        return count == 0
    
    if is_valid(s):
        return [s]
    
    queue = deque([s])
    visited = {s}
    result = []
    found = False
    
    while queue and not found:
        size = len(queue)
        
        for _ in range(size):
            current = queue.popleft()
            
            for i in range(len(current)):
                if current[i] in '()':
                    new_str = current[:i] + current[i+1:]
                    
                    if is_valid(new_str):
                        result.append(new_str)
                        found = True
                    
                    if new_str not in visited:
                        visited.add(new_str)
                        queue.append(new_str)
    
    valid_results = list(set(result)) if result else []
    print(f"Input: '{s}'")
    print(f"Output: {valid_results}")
    return valid_results

# Challenge 96: Longest Increasing Path in Matrix
"""
Problem: Find length of longest increasing path in matrix.
Input: matrix = [[9,9,4],[6,6,8],[2,1,1]]
Expected Output: 4
"""
def challenge_96():
    matrix = [[9,9,4],[6,6,8],[2,1,1]]
    
    if not matrix or not matrix[0]:
        return 0
    
    m, n = len(matrix), len(matrix[0])
    memo = {}
    
    def dfs(i, j):
        if (i, j) in memo:
            return memo[(i, j)]
        
        max_length = 1
        
        for di, dj in [(0, 1), (1, 0), (0, -1), (-1, 0)]:
            ni, nj = i + di, j + dj
            if (0 <= ni < m and 0 <= nj < n and 
                matrix[ni][nj] > matrix[i][j]):
                max_length = max(max_length, 1 + dfs(ni, nj))
        
        memo[(i, j)] = max_length
        return max_length
    
    result = 0
    for i in range(m):
        for j in range(n):
            result = max(result, dfs(i, j))
    
    print(f"Input: {matrix}")
    print(f"Output: {result}")
    return result

# Challenge 97: Frog Jump
"""
Problem: Determine if frog can cross river by jumping on stones.
Input: stones = [0,1,3,5,6,8,12,17]
Expected Output: True
"""
def challenge_97():
    stones = [0,1,3,5,6,8,12,17]
    
    if len(stones) < 2:
        return True
    
    stone_set = set(stones)
    memo = {}
    
    def can_cross(pos, k):
        if pos == stones[-1]:
            return True
        
        if (pos, k) in memo:
            return memo[(pos, k)]
        
        result = False
        for next_k in [k-1, k, k+1]:
            if next_k > 0:
                next_pos = pos + next_k
                if next_pos in stone_set and next_pos <= stones[-1]:
                    if can_cross(next_pos, next_k):
                        result = True
                        break
        
        memo[(pos, k)] = result
        return result
    
    result = can_cross(1, 1)
    print(f"Input: {stones}")
    print(f"Output: {result}")
    return result

# Challenge 98: Design In-Memory File System
"""
Problem: Design in-memory file system with mkdir, addContentToFile, readContentFromFile, ls.
Input: Various file system operations
Expected Output: Results of file system operations
"""
def challenge_98():
    class FileSystem:
        def __init__(self):
            self.files = {}
        
        def ls(self, path):
            if path in self.files and isinstance(self.files[path], str):
                return [path.split('/')[-1]]
            
            directory = self.files.get(path, {})
            return sorted(directory.keys())
        
        def mkdir(self, path):
            current = self.files
            parts = path.split('/')[1:]
            
            for part in parts:
                if part not in current:
                    current[part] = {}
                current = current[part]
        
        def addContentToFile(self, filePath, content):
            parts = filePath.split('/')[1:]
            current = self.files
            
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            filename = parts[-1]
            if filename not in current:
                current[filename] = ""
            current[filename] += content
        
        def readContentFromFile(self, filePath):
            parts = filePath.split('/')[1:]
            current = self.files
            
            for part in parts[:-1]:
                current = current[part]
            
            return current[parts[-1]]
    
    fs = FileSystem()
    operations = [
        ("mkdir", "/a"),
        ("addContentToFile", "/a/b.txt", "hello"),
        ("ls", "/"),
        ("readContentFromFile", "/a/b.txt")
    ]
    
    results = []
    for op in operations:
        if op[0] == "mkdir":
            fs.mkdir(op[1])
            results.append(None)
        elif op[0] == "addContentToFile":
            fs.addContentToFile(op[1], op[2])
            results.append(None)
        elif op[0] == "ls":
            result = fs.ls(op[1])
            results.append(result)
        elif op[0] == "readContentFromFile":
            result = fs.readContentFromFile(op[1])
            results.append(result)
    
    print(f"Input: {len(operations)} file system operations")
    print(f"Output: {results}")
    return results

# Challenge 99: Shortest Path in Binary Matrix
"""
Problem: Find shortest path from top-left to bottom-right in binary matrix.
Input: grid = [[0,0,0],[1,1,0],[1,1,0]]
Expected Output: 4
"""
def challenge_99():
    from collections import deque
    
    grid = [[0,0,0],[1,1,0],[1,1,0]]
    
    if grid[0][0] == 1 or grid[-1][-1] == 1:
        return -1
    
    n = len(grid)
    directions = [(-1,-1), (-1,0), (-1,1), (0,-1), (0,1), (1,-1), (1,0), (1,1)]
    
    queue = deque([(0, 0, 1)])
    visited = {(0, 0)}
    
    while queue:
        row, col, path_len = queue.popleft()
        
        if row == n - 1 and col == n - 1:
            print(f"Input: {grid}")
            print(f"Output: {path_len}")
            return path_len
        
        for dr, dc in directions:
            new_row, new_col = row + dr, col + dc
            
            if (0 <= new_row < n and 0 <= new_col < n and 
                grid[new_row][new_col] == 0 and (new_row, new_col) not in visited):
                
                visited.add((new_row, new_col))
                queue.append((new_row, new_col, path_len + 1))
    
    print(f"Input: {grid}")
    print(f"Output: -1")
    return -1

# Challenge 100: Maximum Frequency Stack
"""
Problem: Design a stack that returns most frequent element when popped.
Input: Operations = [push(5), push(7), push(5), push(7), push(4), push(5), pop(), pop(), pop(), pop()]
Expected Output: [null,null,null,null,null,null,5,7,5,4]
"""
def challenge_100():
    from collections import defaultdict
    
    class FreqStack:
        def __init__(self):
            self.freq = defaultdict(int)
            self.stacks = defaultdict(list)
            self.max_freq = 0
        
        def push(self, val):
            self.freq[val] += 1
            f = self.freq[val]
            self.max_freq = max(self.max_freq, f)
            self.stacks[f].append(val)
        
        def pop(self):
            val = self.stacks[self.max_freq].pop()
            self.freq[val] -= 1
            
            if not self.stacks[self.max_freq]:
                self.max_freq -= 1
            
            return val
    
    freq_stack = FreqStack()
    operations = [
        ("push", 5), ("push", 7), ("push", 5), ("push", 7), 
        ("push", 4), ("push", 5), ("pop",), ("pop",), ("pop",), ("pop",)
    ]
    
    results = []
    for op in operations:
        if op[0] == "push":
            freq_stack.push(op[1])
            results.append(None)
        else:
            result = freq_stack.pop()
            results.append(result)
    
    print(f"Input: {len(operations)} FreqStack operations")
    print(f"Output: {results}")
    return results

# =============================================================================
# MAIN EXECUTION FUNCTIONS
# =============================================================================

def run_all_challenges():
    """Run all 100 challenges"""
    print("="*80)
    print("RUNNING ALL 100 POPULAR PYTHON INTERVIEW CHALLENGES")
    print("="*80)
    
    # Simple Challenges
    print("\n" + "="*50)
    print("SIMPLE CHALLENGES (1-35)")
    print("="*50)
    for i in range(1, 36):
        print(f"\n--- Challenge {i} ---")
        eval(f"challenge_{i}()")
    
    # Intermediate Challenges
    print("\n" + "="*50)
    print("INTERMEDIATE CHALLENGES (36-70)")
    print("="*50)
    for i in range(36, 71):
        print(f"\n--- Challenge {i} ---")
        eval(f"challenge_{i}()")
    
    # Advanced Challenges
    print("\n" + "="*50)
    print("ADVANCED CHALLENGES (71-100)")
    print("="*50)
    for i in range(71, 101):
        print(f"\n--- Challenge {i} ---")
        eval(f"challenge_{i}()")
    
    print("\n" + "="*80)
    print("COMPLETED ALL 100 CHALLENGES!")
    print("="*80)

def run_challenge(n):
    """Run specific challenge"""
    if 1 <= n <= 100:
        print(f"\n--- Challenge {n} ---")
        eval(f"challenge_{n}()")
    else:
        print("Challenge number must be between 1 and 100")

def run_by_difficulty(level):
    """Run challenges by difficulty level"""
    if level == "simple":
        for i in range(1, 36):
            print(f"\n--- Challenge {i} ---")
            eval(f"challenge_{i}()")
    elif level == "intermediate":
        for i in range(36, 71):
            print(f"\n--- Challenge {i} ---")
            eval(f"challenge_{i}()")
    elif level == "advanced":
        for i in range(71, 101):
            print(f"\n--- Challenge {i} ---")
            eval(f"challenge_{i}()")
    else:
        print("Invalid level. Use 'simple', 'intermediate', or 'advanced'")

if __name__ == "__main__":
    print("100 Popular Python Interview Challenges Ready!")
    print("\nUsage:")
    print("run_all_challenges() - Run all 100 challenges")
    print("run_challenge(n) - Run specific challenge (1-100)")
    print("run_by_difficulty('simple') - Run simple challenges (1-35)")
    print("run_by_difficulty('intermediate') - Run intermediate challenges (36-70)")
    print("run_by_difficulty('advanced') - Run advanced challenges (71-100)")
