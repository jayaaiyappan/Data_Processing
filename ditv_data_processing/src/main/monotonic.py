def isMontonic(arr):
    return (all(arr[i] <= arr[i+1] for i in range(len(arr) - 1)))


if __name__ == '__main__':
    myarray = [1,5,6]
    print(isMontonic(myarray))