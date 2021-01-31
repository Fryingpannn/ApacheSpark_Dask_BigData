import sys
sys.path.insert(0, './answers')
from answer import intersection

def test_intersection():
    a = intersection("./data/frenepublicinjection2016.csv", "./data/frenepublicinjection2015.csv")

    try:
        out = open("tests/intersection.txt", "r", encoding="UTF-8").read()
        assert(a == out)
    except:
        out = open("tests/intersection.txt","r", encoding="UTF-8").read()
        assert(a == out)
