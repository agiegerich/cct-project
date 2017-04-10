class Link:
    def __init__(self, name, display):
        self.name = name
        self.display = display

    @classmethod
    def create(cls, name):
        return cls(name, name)

    @classmethod
    def create_with_display(cls, name, display):
        return cls(name, display)

class Article:
    def __init__(self, title, text, links, wordcount):
        self.title = title
        self.text = text
        self.links = links
        self.wordcount = wordcount
