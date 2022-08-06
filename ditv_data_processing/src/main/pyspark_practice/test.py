def solution(s):
    c = s[0]
    if c.isupper():    # please fix condition
        return "upper"
    elif c.islower():  # please fix condition
        return "lower"
    elif c.isdigit():  # please fix condition
        return "digit"
    else:
        return "other"

if __name__ == '__main__':
    # print(solution("@est"))
    # S = "cdeo"
    S = "bytdag"
    # A = [3,2,0,1]
    A = [4,3,0,1,2,5]
    # result = S[0]
    res = S[0]

    # for x in range(0,len(S)):
    #     result = result + S[x]
    counter = 0
    i = A[0]
    while counter < len(A) - 1:
        res = res + S[i]
        next = (A[S.index(S[i])])
        if next == 0:
            break
        i = next
        counter = counter + 1
    print(res)

    # i = A[0]
    # result = result + S[i]
    # next = (A[S.index(S[i])])
    # result = result + S[next]
    # next = (A[S.index(S[next])])
    # result = result + S[next]
    # print(result)



