def checkpair(A,x,size):
    for i in range(0,size-1):
        for j in range(i+1,size):
            if (A[i] + A[j] == x):
                print(A[i], A[j])
                return 1
    return 0

if __name__ == '__main__':
    A = [0, -1, 2, -3, 1]
    sum = -2
    size = len(A)
    if (checkpair(A,sum,size)):
        print("valid pair exists")
    else:
        print("no valid pair exists")