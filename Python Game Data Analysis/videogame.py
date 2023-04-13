class VideoGame: 
    def __init__(self, title, console, loosePrice, completePrice, newPrice):
        self.title = title
        self.console = console
        self.loosePrice = loosePrice
        self.completePrice = completePrice
        self.newPrice = newPrice
    
    def getTitle(self):
        return self.title
    def setTitle(self, title):
        self.title = title

    def getConsole(self):
        return self.console
    def setConsole(self, console):
        self.console = console

    def getLoosePrice(self):
        return self.loosePrice
    def setLoosePrice(self, loosePrice):
        self.loosePrice = loosePrice

    def getCompletePrice(self):
        return self.completePrice
    def setCompletePrice(self, completePrice):
        self.completePrice = completePrice

    def getNewPrice(self):
        return self.completePrice
    def setNewPrice(self, newPrice):
        self.completePrice = newPrice

    def printVals(self):
        print("Title: {}\nConsole: {}\nLoose Price: ${}\nComplete Price: ${}\nNew Price: ${}\n\n".format(self.title, self.console, self.loosePrice, self.completePrice, self.newPrice))