def greet(name):
    print("Hello, " + name + ". Good afternoon!")
    
def greeteve(name):
    print("Hello, " + name + ". Good Evening !")

    
def greetmor(name):
    """
    This function executes other fuctions and its own
    """
    greet('saishashank')
    greeteve('shashank')
    print("Hello, " + name + ". Good Morning!")
