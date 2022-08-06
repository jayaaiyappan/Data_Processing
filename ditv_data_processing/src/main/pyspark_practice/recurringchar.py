def recurring_char(input):
    seen = set()
    for char in input:
        if char in seen:
            return char
        seen.add(char)

    return(None)

if __name__ == '__main__':
    print(recurring_char('interview'))