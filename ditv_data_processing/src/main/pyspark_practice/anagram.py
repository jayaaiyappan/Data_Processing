# https://github.com/donnemartin/interactive-coding-challenges/blob/master/sorting_searching/anagrams/anagrams_solution.ipynb

# from collections import OrderedDict

class Anagram:
    def group_anagrams(self, items):
        if items is None:
            raise TypeError('item cannot be none')
        if not items: # check if the list is empty
            return items;
        # anagram_map = OrderedDict()
        anagram_map = {}
        for item in items:
            sorted_chars = tuple(sorted(item))
            # print(sorted_chars)
            if sorted_chars in anagram_map:
                print(anagram_map[sorted_chars])
                anagram_map[sorted_chars].append(item)
            else:
                anagram_map[sorted_chars] = [item]
        result = []
        for value in anagram_map.values():
            result.extend(value)
        print(result)



def main():
    anagram = Anagram()
    anagram.group_anagrams(['cat','pat','mat','tap'])


if __name__ == '__main__':
    main()