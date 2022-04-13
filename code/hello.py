# Test script for readthedocs (can be removed at will)

import sys

def main(name):
    say_hello(name)


def say_hello(name):
    """
    Prints hello using the name parameter

    :param name: person who says hello
    """

    print(name + " says hello!")

if __name__ == "__main__":
    if (len(sys.argv) == 1):
        print ("No name supplied. Nobody says hello...")
    else:
        main(sys.argv[1])