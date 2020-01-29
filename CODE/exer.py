import sys

#now = datetime.datetime.now()
#print(now.strftime("%Y:%m:d% - %H:%M:%S"))
#i=([i**2 for i in range(10000)])

#GENERATION IMPLEMENTATION
class firstn(object):
    def __init__(self,n):
        self.n = n
        self.num, self.nums = 0, []

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.num < self.n:
            curr, self.num = self.num, self.num+1
            return curr
        else:
            print('END')
            raise StopIteration()

def firstn2(n):
    num = 0
    while num < n:
        yield num
        num += 1

if __name__=="__main__":
    a = sys.argv[1]
    f = firstn(int(a))
    for i in f:
        print(i)
    print(f)
    print(sum(firstn2(100)))

