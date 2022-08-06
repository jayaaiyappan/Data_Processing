# Bigram formation
# using list comprehension + enumerate() + split()

# initializing list 
test_list = ['geeksforgeeks is best', 'I love it']

# printing the original list
print("The original list is : " + str(test_list))

# using list comprehension + enumerate() + split()
# for Bigram formation
res = [(x, i.split()[j + 1]) for i in test_list
       for j, x in enumerate(i.split()) if j < len(i.split()) - 1]

# printing result
print("The formed bigrams are : " + str(res))

res1 = []
for item in test_list:
       for i,x in enumerate(item.split()):
              if i < len(item.split()) - 1:
                     res1.append((x,item.split()[i+1]))

print(res1)

res2  = [(x,item.split()[i+1]) for item in test_list for i,x in enumerate(item.split()) if i < len(item.split()) -1]
print(res2)


sentence = "this is practice session"

res3 = [(word,sentence.split()[i+1]) for i,word in enumerate(sentence.split()) if i < len(sentence.split()) - 1]
print(res3)